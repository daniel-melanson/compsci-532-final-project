import database as db
import os

def main():
    if not os.path.exists(db.DB_FILE_PATH):
        db.init()


if __name__ == "__main__":
    main()