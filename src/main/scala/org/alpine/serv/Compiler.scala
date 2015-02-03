package org.alpine.serv

import scala.tools.nsc.{ Global, Settings }
import tools.nsc.util.BatchSourceFile
import tools.nsc.io.{ VirtualDirectory, AbstractFile }
import tools.nsc.interpreter.AbstractFileClassLoader
import java.security.MessageDigest
import java.math.BigInteger
import collection.mutable
import java.io.File
import scala.util.{ Failure, Success, Try }

/**
 * The Compiler class facilitates compiling and classloading arbitrary Scala code at runtime.
 *
 * Taken from https://eknet.org/main/dev/runtimecompilescala.html, which was originally taken from Twitter's util-eval.
 *
 * @param targetDir The location of the compiled Scala classes. If None, then the code is compilied into and loaded from memory.
 */
class Compiler(targetDir: Option[File]) {

  /** The location of compiled classes. If it's a virtual directory, then it means that we're doing it all in memory. */
  val target = targetDir match {
    case Some(dir) => AbstractFile.getDirectory(dir)
    case None      => new VirtualDirectory("(memory)", None)
  }

  /** The classloader that this Compiler is using. */
  val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)

  private val classCache = mutable.Map[String, Class[_]]()

  private val global = {
    val settings = new Settings()
    settings.deprecation.value = true // enable detailed deprecation warnings
    settings.unchecked.value = true // enable detailed unchecked warnings
    settings.outputDirs.setSingleOutput(target)
    settings.usejavacp.value = true

    new Global(settings)
  }

  private lazy val run = new global.Run

  /** Compiles the code as a class into the class loader of this compiler. */
  def compile(code: String): Class[_] = {
    val className = classNameForCode(code)
    findClass(className).getOrElse {
      val sourceFiles = List(new BatchSourceFile("(inline)", wrapCodeInClass(className, code)))
      run.compileSources(sourceFiles)
      findClass(className).get
    }
  }

  /** Compiles the source string into the class loader and evaluates it. */
  def eval[T](code: String): T =
    compile(code)
      .getConstructor().newInstance()
      .asInstanceOf[() => Any].apply().asInstanceOf[T]

  /**
   * Returns the class object indicated by the class as specified in the input string.
   * If there is no class, then findClass returns None, otherwise it returns a Some containing the class.
   */
  def findClass(className: String): Option[Class[_]] =
    synchronized {
      classCache.get(className) match {
        case someClazz: Some[Class[_]] =>
          someClazz

        case None =>
          Try(classLoader.loadClass(className)) match {
            case Success(clazz) =>
              classCache(className) = clazz
              Some(clazz)
            case Failure(_) =>
              None
          }
      }
    }

  private val digester = MessageDigest.getInstance("SHA-1")

  /** Compute a SHA-1 hash of the code and use it to create a unique class name */
  def classNameForCode(code: String): String =
    s"sha${new BigInteger(1, digester.digest(code.getBytes)).toString(16)}"

  /** Wrap source code in a new class with an apply method. */
  def wrapCodeInClass(className: String, code: String): String =
    s"""
class $className extends (() => Any) {
  override def apply():Any = {
    $code
  }
}""".stripMargin.trim

}
