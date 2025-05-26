from datetime import datetime, timedelta
import csv
import random
import uuid
import os
import argparse

CATEGORIES = ['Art', 'Technology', 'Games', 'Music', 'Film', 'Publishing']
STATES = ['successful', 'failed', 'canceled']
COUNTRIES = ['US', 'GB', 'CA', 'DE', 'FR']
CURRENCIES = ['USD', 'GBP', 'CAD', 'EUR']
CATEGORY_MAPPING = {
    'Art': ['Painting', 'Sculpture'],
    'Technology': ['Gadgets', 'Wearables'],
    'Games': ['Video Games', 'Board Games'],
    'Music': ['Rock', 'Jazz'],
    'Film': ['Documentary', 'Short'],
    'Publishing': ['Fiction', 'Non-fiction']
}

RAW_DIR = "data/raw/"

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_kickstarter_csv(rows):
    now = datetime.now()
    filename = f"ks-projects-{now.strftime('%Y%m%d%H%M%S')}.csv"
    filepath = os.path.join(RAW_DIR, filename)

    os.makedirs(RAW_DIR, exist_ok=True)

    with open(filepath, mode="w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "ID", "name", "category", "main_category", "currency", "deadline", "goal",
            "launched", "pledged", "state", "backers", "country", "usd pledged"
        ])

        for i in range(rows):
            project_id = str(uuid.uuid4().int)[:9]
            main_cat = random.choice(CATEGORIES)
            cat = random.choice(CATEGORY_MAPPING[main_cat])
            goal = random.randint(50000, 1500000)
            pledged = int(goal * random.uniform(0.3, 1.5))
            backers = random.randint(5, 5000)
            launched = random_date(now - timedelta(days=365 * 4), now - timedelta(days=10))
            deadline = launched + timedelta(days=random.randint(10, 60))

            writer.writerow([
                str(project_id),
                f"Project {project_id}",
                cat,
                main_cat,
                random.choice(CURRENCIES),
                deadline.strftime("%Y-%m-%d"),
                goal,
                launched.strftime("%Y-%m-%d %H:%M:%S"),
                pledged,
                random.choices(STATES, weights=[0.4, 0.5, 0.1])[0],
                backers,
                random.choice(COUNTRIES),
                int(pledged * random.uniform(0.9, 1.1))
            ])
    print(f"Generated: {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fake Kickstarter data.")
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    generate_kickstarter_csv(rows=args.rows)
