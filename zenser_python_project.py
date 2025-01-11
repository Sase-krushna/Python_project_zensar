import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import mysql.connector
from datetime import date, datetime
from decimal import Decimal

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "zxr123",
    "database": "moviesdb"
}

# Get a database connection
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Custom JSON encoder to handle special data types
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()  # Convert to ISO 8601 string
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        return super().default(obj)

# Request Handler
class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with get_db_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    if self.path == "/movies":
                        # Fetch all movies
                        cursor.execute("SELECT * FROM movies")
                        result = cursor.fetchall()
                    elif self.path.startswith("/recommend/"):
                        # Fetch recommendations for a user
                        user_id = self.path.split("/")[-1]
                        cursor.execute("""
                            SELECT m.movie_id, m.title 
                            FROM movies m
                            WHERE m.movie_id NOT IN (
                                SELECT r.movie_id 
                                FROM ratings r 
                                WHERE r.user_id = %s
                            )
                            LIMIT 5
                        """, (user_id,))
                        result = cursor.fetchall()
                    else:
                        self.send_error(404, "Invalid endpoint")
                        return

            # Respond with the query result
            response_body = json.dumps(result, cls=CustomJSONEncoder)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response_body.encode())
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")

    def do_POST(self):
        try:
            # Read and parse the request body
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))

            if self.path == "/rate_movie":
                # Insert or update a movie rating
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO ratings (user_id, movie_id, rating)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE rating = %s
                        """, (data['user_id'], data['movie_id'], data['rating'], data['rating']))
                        conn.commit()

                # Respond with success
                self.send_response(201)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {"message": "Rating submitted successfully"}
                self.wfile.write(json.dumps(response).encode())
            else:
                self.send_error(404, "Invalid endpoint")
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")

# Run the HTTP server
def run(server_class=HTTPServer, handler_class=RequestHandler, port=8080):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    print(f"Server started on port {port}")
    httpd.serve_forever()

if __name__ == "__main__":
    run()
