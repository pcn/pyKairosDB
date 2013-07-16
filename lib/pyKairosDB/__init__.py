import connection
import util
import metadata

def connect(server='localhost', port='8080', ssl=False):
    return connection.KairosDBConnection(server, port, ssl)



__all__ = ["connect", "util", "metadata"]