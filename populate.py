import psycopg2
import random
from faker import Faker
from datetime import datetime, timedelta

# Database connection settings
DB_CONFIG = {
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": "5432",
}

fake = Faker()

def populate_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Populate Date Dimension Table with 10 years of dates
        start_date = datetime(2014, 1, 1)
        for single_date in (start_date + timedelta(n) for n in range(3650)):
            date_id = (single_date - start_date).days + 1
            cursor.execute("""
                INSERT INTO date_dim (date_id, date, year, month, weekday)
                VALUES (%s, %s, %s, %s, %s)
            """, (date_id, single_date.date(), single_date.year, single_date.month, single_date.strftime("%A")))

        print("Date dimension table populated successfully!")

        # Generate Directors
        director_ids = []
        for _ in range(100):
            director_name = fake.name()[:100]
            cursor.execute("INSERT INTO director_dim (name) VALUES (%s) RETURNING director_id", (director_name,))
            director_ids.append(cursor.fetchone()[0])
        print("Director dimension table populated successfully!")

        # Generate Movies
        movie_ids = []
        for _ in range(500):
            title = fake.sentence(nb_words=3)[:100]
            genre = random.choice(["Action", "Drama", "Comedy", "Horror", "Sci-Fi"])[:100]
            director_id = random.choice(director_ids)
            cursor.execute("INSERT INTO movie_dim (title, genre, director_id) VALUES (%s, %s, %s) RETURNING movie_id", 
                           (title, genre, director_id))
            movie_ids.append(cursor.fetchone()[0])
        print("Movie dimension table populated successfully!")

        # Generate Customers
        customer_ids = []
        for _ in range(1000):
            name = fake.name()
            gender = random.choice(["M", "F", "O"])
            dob = fake.date_of_birth(minimum_age=10, maximum_age=90)
            cursor.execute("INSERT INTO customer_dim (name, gender, dob) VALUES (%s, %s, %s) RETURNING customer_id", 
                           (name, gender, dob))
            customer_ids.append(cursor.fetchone()[0])
        print("Customer dimension table populated successfully!")

        # Generate Cinemas
        cinema_ids = []
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "San Francisco"]
        for _ in range(50):
            name = fake.company()[:100]
            city = random.choice(cities)
            state = fake.state()
            hall_size = random.randint(50, 300)
            cursor.execute("INSERT INTO cinema_dim (name, city, state, hall_size) VALUES (%s, %s, %s, %s) RETURNING cinema_id", 
                           (name, city, state, hall_size))
            cinema_ids.append(cursor.fetchone()[0])
        print("Cinema dimension table populated successfully!")

        # Generate Transactions (Reduced to 1000000 rows)
        for _ in range(1000000):
            customer_id = random.choice(customer_ids)
            movie_id = random.choice(movie_ids)
            cinema_id = random.choice(cinema_ids)
            cursor.execute("SELECT date_id FROM date_dim ORDER BY RANDOM() LIMIT 1")
            date_id = cursor.fetchone()[0]
            totalprice = round(random.uniform(5, 50), 2)
            ticket_count = random.randint(1, 5)
            showtime = fake.time()

            cursor.execute("""
                INSERT INTO transaction_fact (customer_id, movie_id, cinema_id, date_id, totalprice, ticket_count, showtime)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (customer_id, movie_id, cinema_id, date_id, totalprice, ticket_count, showtime))

        conn.commit()
        print("Database populated successfully!")

    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()

# Run script
populate_database()
