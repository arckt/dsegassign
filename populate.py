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

# Function to insert synthetic data
def insert_data(chunk):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for query, params in chunk:
            cursor.execute(query, params)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error:", e)

# Function to populate the database
def populate_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Populate Date Dimension Table
        start_date = datetime(2014, 1, 1)
        date_data = [("INSERT INTO date_dim (date_id, date, year, month, weekday) VALUES (%s, %s, %s, %s, %s)",
                      (date_id, single_date.date(), single_date.year, single_date.month, single_date.strftime("%A")))
                     for date_id, single_date in enumerate((start_date + timedelta(n) for n in range(3650)), start=1)]

        insert_data(date_data)
        print("Date dimension table populated successfully!")

        # Generate Directors
        director_data = {}
        for _ in range(200):
            director_name = fake.name()
            cursor.execute("INSERT INTO director_dim (name) VALUES (%s) RETURNING director_id", (director_name,))
            director_id = cursor.fetchone()[0]
            director_data[director_id] = director_name

        print("Director dimension table populated successfully!")

        # Generate Stars
        star_data = {}
        for _ in range(200):
            star_name = fake.name()
            cursor.execute("INSERT INTO star_dim (star_name) VALUES (%s) RETURNING star_id", (star_name,))
            star_id = cursor.fetchone()[0]
            star_data[star_id] = star_name

        print("Star dimension table populated successfully!")

        # Generate Movies
        movie_data = []
        for _ in range(500):
            title = fake.sentence(nb_words=3)[:100]
            genre = random.choice(["Action", "Drama", "Comedy", "Horror", "Sci-Fi"])
            director_id = random.choice(list(director_data.keys()))
            director_name = director_data[director_id]

            stars = random.sample(list(star_data.keys()), random.randint(1, 5))
            star_names = [star_data[star_id] for star_id in stars]
            star_names += [None] * (5 - len(star_names))

            cursor.execute(
                "INSERT INTO movie_dim (title, genre, director_name, star1_name, star2_name, star3_name, star4_name, star5_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING movie_id",
                (title, genre, director_name, star_names[0], star_names[1], star_names[2], star_names[3], star_names[4])
            )
            movie_data.append(cursor.fetchone()[0])

        print("Movie dimension table populated successfully!")

        # Generate Cinemas
        cinema_data = []
        for _ in range(50):
            name = fake.company()
            city = fake.city()
            state = fake.state()
            hall_size = random.randint(50, 500)
            cursor.execute("INSERT INTO cinema_dim (name, city, state, hall_size) VALUES (%s, %s, %s, %s) RETURNING cinema_id", (name, city, state, hall_size))
            cinema_data.append(cursor.fetchone()[0])

        print("Cinema dimension table populated successfully!")

        # Generate Promotions
        promotion_data = []
        for _ in range(10):
            description = fake.sentence(nb_words=4)
            cursor.execute("INSERT INTO promotion_dim (description) VALUES (%s) RETURNING promotion_id", (description,))
            promotion_data.append(cursor.fetchone()[0])

        print("Promotion dimension table populated successfully!")

        # Generate Online Transactions
        online_transaction_data = []
        for _ in range(100):
            browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
            cursor.execute("INSERT INTO online_transaction_dim (browser) VALUES (%s) RETURNING online_transaction_id", (browser,))
            online_transaction_data.append(cursor.fetchone()[0])

        print("Online transaction dimension table populated successfully!")

        # Generate Customers
        customer_data = []
        for _ in range(1000):
            name = fake.name()
            gender = random.choice(["M", "F", "O"])
            dob = fake.date_of_birth(minimum_age=10, maximum_age=90)
            cursor.execute("INSERT INTO customer_dim (name, gender, dob) VALUES (%s, %s, %s) RETURNING customer_id", (name, gender, dob))
            customer_data.append(cursor.fetchone()[0])

        print("Customer dimension table populated successfully!")

        #Ensure customer data is committed before transaction generation.
        conn.commit()

        # Generate Transactions
        transaction_data = []
        for _ in range(1000000):
            customer_id = random.choice(customer_data)
            movie_id = random.choice(movie_data)
            cinema_id = random.choice(cinema_data)
            date_id = random.choice(range(1, 3651))  # Adjust this to match your date_dim entries
            promotion_id = random.choice(promotion_data)
            totalprice = round(random.uniform(5, 50), 2)
            ticket_count = random.randint(1, 5)
            online_transaction_id = random.choice(online_transaction_data)
            offline_transaction_id = random.randint(1000, 9999)
            showtime = fake.time()

            transaction_data.append(("INSERT INTO transaction_fact (customer_id, movie_id, cinema_id, date_id, promotion_id, totalprice, ticket_count, online_transaction_id, offline_transaction_id, showtime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                     (customer_id, movie_id, cinema_id, date_id, promotion_id, totalprice, ticket_count, online_transaction_id, offline_transaction_id, showtime)))

        # Insert transaction data
        insert_data(transaction_data)
        print("Transaction fact table populated successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()

# Run script
populate_database()
