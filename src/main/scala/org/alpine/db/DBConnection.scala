package org.alpine.db

sealed trait DBConnection {
  def connectionName: String
  def user: UserInfo
  def useSSL: Boolean
  def useSID: Boolean
  def url: String
  def schemaBlackList: Set[String]
  def network: NetworkConnection
}

case class UserInfo(username: String, password: String)

case class NetworkConnection(hostname: String, port: Int) {
  override def toString: String = s"$hostname:$port"
}

case class OracleConnection(
  connectionName: String,
  user: UserInfo,
  useSSL: Boolean,
  useSID: Boolean,
  url: String,
  schemaBlackList: Set[String],
  network: NetworkConnection) extends DBConnection

case class PostgresConnection(
  connectionName: String,
  user: UserInfo,
  useSSL: Boolean,
  useSID: Boolean,
  url: String,
  schemaBlackList: Set[String],
  network: NetworkConnection) extends DBConnection

case class GreenplumConnection(
  connectionName: String,
  user: UserInfo,
  useSSL: Boolean,
  useSID: Boolean,
  url: String,
  schemaBlackList: Set[String],
  network: NetworkConnection) extends DBConnection

case class JDBCConnection(
  connectionName: String,
  user: UserInfo,
  useSSL: Boolean,
  useSID: Boolean,
  url: String,
  schemaBlackList: Set[String],
  network: NetworkConnection,
  jdbcDriver: Class[_]) extends DBConnection

