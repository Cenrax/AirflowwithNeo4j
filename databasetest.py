from neo4jconnection import Neo4jConnection
conn = Neo4jConnection(uri="neo4j://localhost:7687", user="subham", pwd="subham")

query_string = '''CREATE (rohit:player{name: "Rohit @Kohli", YOB: 1985, POB: "Bangalore"})'''
conn.query(query_string, db='demodb')
conn.close()