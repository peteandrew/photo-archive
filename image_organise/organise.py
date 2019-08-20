import sys
from PIL import Image
from PIL.ExifTags import TAGS
from os import listdir, makedirs
from os.path import isfile, exists
import shutil
import sqlite3

dest = '/mnt/external/pictures/processed/'

if len(sys.argv) < 2:
    print('source image directory required')
    sys.exit(1)

dir = sys.argv[1]
if dir[-1] != '/':
    dir += '/'

if not exists(dir):
    print('source image directory does not exist')
    sys.exit(1)

db_conn = sqlite3.connect('images.db')
c = db_conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS images (id integer primary key, filename text, time_created text, time_processed text, original_directory text)")
db_conn.commit()

files = [f for f in listdir(dir) if isfile(dir + f) and f[-3:].lower() == 'jpg']

for file in files:
    print(file)
    
    full_src = dir + file
    image = Image.open(full_src)

    info = image._getexif()
    if not info:
        print("no info\n")
        continue

    exif_data = {}
    for tag, value in info.items():
        decoded = TAGS.get(tag, tag)
        exif_data[decoded] = value

    try:
        date_time = exif_data['DateTimeOriginal']
        year = date_time[:4]
        month = date_time[5:7]
        time_created = year + '-' + month + '-' + date_time[8:]
        print(time_created)

        c = db_conn.cursor()
        c.execute("SELECT * FROM images WHERE filename = ? AND time_created = ?", (file, time_created,))
        existing_images = c.fetchall()
        if len(existing_images) > 0:
            print("already exists, skipping")
            print(existing_images)
            print('')
            continue

        dest_dir = dest + year + '/' + month

        if not exists(dest_dir):
            makedirs(dest_dir)

        full_dest = dest_dir + '/' + file

        shutil.move(full_src, full_dest)

        c = db_conn.cursor()
        c.execute("INSERT INTO images (filename, time_created, time_processed, original_directory) VALUES (?, ?, datetime('now'), ?)", (file, time_created, dir,))
        db_conn.commit()

    except KeyError:
        print('no DateTimeOriginal')

    print('')

db_conn.close()
