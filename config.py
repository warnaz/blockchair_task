import os
from dotenv import load_dotenv


load_dotenv()

address = os.getenv("ADDRESS")
keys = (os.getenv("LOGIN"), os.getenv("PASSWORD"))
PROXY = os.getenv("PROXY")