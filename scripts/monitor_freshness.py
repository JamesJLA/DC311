import psycopg2
import smtplib
import os
from datetime import datetime, timezone
from email.mime.text import MIMEText
from dotenv import load_dotenv
from dateutil import parser

load_dotenv()  # Load EMAIL_USER and EMAIL_PASS from .env

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")

def send_email_alert(subject, body):
    sender = EMAIL_USER
    recipient = EMAIL_USER
    password = EMAIL_PASS

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print("✅ Alert email sent.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def check_data_freshness():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="monitor_db",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(created_date) FROM dc_311_requests")
        latest_date = cursor.fetchone()[0]

        if latest_date is None:
            print("⚠️ No data found in dc_311_requests table.")
            return False

        # Handle if latest_date is int or float (timestamp in ms or s)
        if isinstance(latest_date, (int, float)):
            if latest_date > 1e12:  # milliseconds timestamp
                latest_date = datetime.fromtimestamp(latest_date / 1000, tz=timezone.utc)
            else:  # seconds timestamp
                latest_date = datetime.fromtimestamp(latest_date, tz=timezone.utc)
        elif not isinstance(latest_date, datetime):
            # Try parsing as string date
            try:
                latest_date = parser.parse(str(latest_date))
            except Exception as e:
                print(f"❌ Failed to parse latest_date: {e}")
                return False

        # Ensure timezone aware datetime (assume UTC if naive)
        if latest_date.tzinfo is None:
            latest_date = latest_date.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        age_days = (now - latest_date).days

        if age_days > 2:
            print(f"❗ Data is stale — latest entry is {age_days} days old ({latest_date})")
            send_email_alert(
                subject="🚨 DC311 Data Stale Alert",
                body=f"The latest service request is {age_days} days old (Last entry: {latest_date})."
            )
        else:
            print(f"✅ Data is fresh — latest entry: {latest_date}")
        return True

    except Exception as e:
        print(f"❌ Error checking freshness: {e}")
        return False

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
