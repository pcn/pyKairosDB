import connection
import util
import metadata

def connect(server='localhost', port='8080', ssl=False):
    """
    :type server: str
    :param server: the host to connect to that is running KairosDB
    :type port: str
    :param port: the port, as a string, that the KairosDB instance is running on
    :type ssl: bool
    :param ssl: Whether or not to use ssl for this connection.

    :rtype: KairosDBConnection
    :return: A connection object to the database

    This wraps the pyKairosDB.connection.KairosDBConnection constructor and returns an
    instance of that class.
    """
    return connection.KairosDBConnection(server, port, ssl)



__all__ = ["connect", "util", "metadata"]