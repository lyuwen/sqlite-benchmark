import io
import os
import time
import string
import sqlite3
import textwrap
import numpy as np

def adapt_array(arr):
  """
  Converts np.array to TEXT when inserting

  http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
  """
  out = io.BytesIO()
  np.save(out, arr)
  out.seek(0)
  return sqlite3.Binary(out.read())


def convert_array(text):
  """
  Converts TEXT to np.array when selecting
  """
  out = io.BytesIO(text)
  out.seek(0)
  return np.load(out, allow_pickle=True)


def generate_random_text(length):
  return "".join(np.array(list(string.printable))[np.random.randint(0, high=len(string.printable), size=(200, ))])


sqlite3.register_adapter(np.ndarray, adapt_array)

sqlite3.register_converter("array", convert_array)

def main():
  db_name = "database.db"
  niter= 500

  if os.path.exists(db_name):
    os.remove(db_name)
  db = sqlite3.connect(db_name)

  sql_create_table = """CREATE TABLE IF NOT EXISTS test_table (
    id integer PRIMARY KEY,
    name text NOT NULL,
    text1 text NOT NULL,
    random1 float not NULL,
    random2 float not NULL,
    random3 int not NULL,
    random4 array
    );"""

  print("Create table with the following parameters:")
  print(textwrap.indent(sql_create_table, "  "))

  start = time.time()

  db.cursor().execute(sql_create_table)
  db.commit()

  print("Time elapsed: {} seconds".format(time.time() - start))
  start = time.time()

  sql_insert = '''INSERT INTO test_table(id,name,text1,random1,random2,random3,random4) VALUES(?,?,?,?,?,?,?);'''

  index = 0

  print(f"Insert {niter} items to the test_table with commit after each insertion.")
  for i in range(niter):
    index += 1
    values = (
        index, # id
        f"test {index}", # name
        generate_random_text(100), # text 1
        np.random.rand(), # random 1
        np.round(np.random.rand() * 100).astype(int), # random 2
        np.random.randint(low=0, high=10, size=(3, 3)), # random 3
        np.random.random((3, 10)), # random 4
        )
    db.cursor().execute(sql_insert, values)
    db.commit()

  print("Time elapsed: {:6.3f} seconds".format(time.time() - start))
  start = time.time()

  print(f"Insert {niter} items to the test_table without commit after each insertion.")
  for i in range(niter):
    index += 1
    values = (
        index, # id
        f"test {index}", # name
        generate_random_text(100), # text 1
        np.random.rand(), # random 1
        np.round(np.random.rand() * 100).astype(int), # random 2
        np.random.randint(low=0, high=10, size=(3, 3)), # random 3
        np.random.random((3, 10)), # random 4
        )
    db.cursor().execute(sql_insert, values)
  db.commit()

  print("Time elapsed: {:6.3f} seconds".format(time.time() - start))

if __name__ == "__main__":
  main()
