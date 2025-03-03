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
def populate_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Populate Date Dimension Table with 10 years of dates
        start_date = datetime(2015, 1, 1)
        end_date = start_date + timedelta(days=3650)  # 10 years later

        for single_date in (start_date + timedelta(n) for n in range(3650)):
            date_id = (single_date - start_date).days + 1  # Generate a date_id from 1 to 3650
            year = single_date.year
            month = single_date.month
            weekday = single_date.strftime("%A")
            cursor.execute("""
                INSERT INTO date_dim (date_id, date, year, month, weekday)
                VALUES (%s, %s, %s, %s, %s)
            """, (date_id, single_date.date(), year, month, weekday))

        print("Date dimension table populated successfully!")

        # Generate Customers
        customer_ids = []
        for _ in range(50000):
            name = fake.name()
            gender = random.choice(["M", "F", "O"])
            dob = fake.date_of_birth(minimum_age=10, maximum_age=90)
            cursor.execute("INSERT INTO customer_dim (name, gender, dob) VALUES (%s, %s, %s) RETURNING customer_id", 
                           (name, gender, dob))
            customer_id = cursor.fetchone()[0]
            customer_ids.append(customer_id)  # Collect customer_ids to use in transactions

        print("Customer dimension table populated successfully!")

        # Generate Movies
        movie_ids = []
        for _ in range(500):
            title = fake.sentence(nb_words=3)[:100]  # Ensure the title doesn't exceed 100 characters
            genre = random.choice(["Action", "Drama", "Comedy", "Horror", "Sci-Fi"])[:100]  # Ensure the genre doesn't exceed 100 characters
            director_name = fake.name()[:100]  # Ensure the director's name doesn't exceed 100 characters
            cursor.execute("INSERT INTO movie_dim (title, genre, director_name) VALUES (%s, %s, %s) RETURNING movie_id", 
                           (title, genre, director_name))
            movie_id = cursor.fetchone()[0]
            movie_ids.append(movie_id)  # Collect movie_ids to use in the next step

        print("Movie dimension table populated successfully!")

        # Generate Movie Stars
        star_ids = []
        for _ in range(200):  # 200 unique stars
            star_name = fake.name()
            cursor.execute("INSERT INTO star_dim (star_name) VALUES (%s) RETURNING star_id", (star_name,))
            star_id = cursor.fetchone()[0]
            star_ids.append(star_id)

        print("Star dimension table populated successfully!")

        # Generate Movie-Star Relationships (Junction Table)
        for movie_id in movie_ids:  # Movie IDs that were inserted earlier
            num_stars = random.randint(1, 5)  # Each movie has 1-5 stars
            chosen_stars = random.sample(star_ids, num_stars)
            for star_id in chosen_stars:
                cursor.execute("INSERT INTO movie_star_dim (movie_id, star_id) VALUES (%s, %s)", (movie_id, star_id))

        print("Movie-Star relationships populated successfully!")

        # Generate Cinemas and collect their IDs
        cinema_ids = []
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "San Francisco"]
        for _ in range(100):
            name = fake.company()[:100]  # Ensure the company name doesn't exceed 100 characters
            city = random.choice(cities)[:100]  # Ensure the city doesn't exceed 100 characters
            state = fake.state()[:100]  # Ensure the state doesn't exceed 100 characters
            hall_size = random.randint(50, 300)
            cursor.execute("INSERT INTO cinema_dim (name, city, state, hall_size) VALUES (%s, %s, %s, %s) RETURNING cinema_id", 
                           (name, city, state, hall_size))
            cinema_id = cursor.fetchone()[0]
            cinema_ids.append(cinema_id)  # Collect cinema_ids to use in transactions

        print("Cinema dimension table populated successfully!")

        # Populate Promotion Dimension Table
        promotion_ids = []
        promotion_descriptions = ["Discount", "New Release", "Seasonal Offer", "Special Price", "Loyalty Bonus"]
        for desc in promotion_descriptions:
            cursor.execute("INSERT INTO promotion_dim (description) VALUES (%s) RETURNING promotion_id", (desc,))
            promotion_id = cursor.fetchone()[0]
            promotion_ids.append(promotion_id)  # Collect promotion_ids to use in transactions

        print("Promotion dimension table populated successfully!")

        # Populate Online Transaction Dimension Table
        online_transaction_ids = []
        for _ in range(50000):  # Generate 50k online transactions
            browser = fake.user_agent()[:100]  # Ensure the browser value doesn't exceed 100 characters
            cursor.execute("INSERT INTO online_transaction_dim (browser) VALUES (%s) RETURNING online_transaction_id", 
                           (browser,))
            online_transaction_id = cursor.fetchone()[0]
            online_transaction_ids.append(online_transaction_id)  # Collect online_transaction_ids to use in transactions

        print("Online transaction dimension table populated successfully!")

        # Generate Transactions
        for _ in range(1000000):  # 1M rows
            customer_id = random.choice(customer_ids)  # Choose a customer_id from the populated list
            movie_id = random.choice(movie_ids)  # Choose a movie_id from the populated list
            cinema_id = random.choice(cinema_ids)  # Choose a cinema_id from the populated list
            
            # Get a random date_id that exists in the date_dim table
            cursor.execute("SELECT date_id FROM date_dim ORDER BY RANDOM() LIMIT 1")
            date_id = cursor.fetchone()[0]
            
            # Choose a random promotion_id from the populated promotion_ids
            promotion_id = random.choice(promotion_ids)

            # Randomly choose an online transaction ID if online transaction is present
            online_transaction_id = random.choice(online_transaction_ids) if random.random() > 0.5 else None
            offline_transaction_id = random.randint(1, 50000) if online_transaction_id is None else None

            totalprice = round(random.uniform(5, 50), 2)
            ticket_count = random.randint(1, 5)
            showtime = fake.time()

            cursor.execute("""
                INSERT INTO transaction_fact (customer_id, movie_id, cinema_id, date_id, promotion_id, totalprice, ticket_count, online_transaction_id, offline_transaction_id, showtime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (customer_id, movie_id, cinema_id, date_id, promotion_id, totalprice, ticket_count, online_transaction_id, offline_transaction_id, showtime))

        conn.commit()
        print("Database populated successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()

# Run script
populate_database()
