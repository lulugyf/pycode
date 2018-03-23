
from mysql.connector import MySQLConnection


def main():
    co = MySQLConnection(host='10.162.201.242', port=8066,
                         user='informdb', password='Informdb#123' )
    cur = co.cursor()
    cur.close()
    co.close()

if __name__ == '__main__':
    main()
