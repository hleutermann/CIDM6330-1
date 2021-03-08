# the database module is much more testable as its actions are largely atomic
# that said, the database module could certain be refactored to achieve decoupling
# in fact, either the implementation of the Unit of Work or just changing to sqlalchemy would be good.

import os
from datetime import datetime
import sqlite3

import pytest

from database import DatabaseManager

@pytest.fixture
def database_manager() -> DatabaseManager:
    """
    What is a fixture? https://docs.pytest.org/en/stable/fixture.html#what-fixtures-are
    """
    filename = "test_bookmarks.db"
    dbm = DatabaseManager(filename)
    yield dbm
    dbm.__del__()           # explicitly release the database manager
    os.remove(filename)

def test_database_manager_create_table(database_manager):
    # arrange and act
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    #assert
    conn = database_manager.connection
    cursor = conn.cursor()
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='bookmarks' ''')
    assert cursor.fetchone()[0] == 1

    #cleanup
    cursor.close()
    database_manager.drop_table("bookmarks")

# CREATE A TEST FOR DROP_TABLE
def test_database_manager_drop_table(database_manager):
    # arrange and act
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    database_manager.drop_table("bookmarks")

    #assert
    conn = database_manager.connection
    cursor = conn.cursor()
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='bookmarks' ''')
    assert cursor.fetchone()[0] == 0     # this proves that the table called "bookmarks" no longer exists

    # cleanup
    cursor.close()

def test_database_manager_add_bookmark(database_manager):

    # arrange
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    data = {
        "title": "test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    # act
    database_manager.add("bookmarks", data)

    # assert
    conn = database_manager.connection
    cursor = conn.cursor()
    cursor.execute(''' SELECT * FROM bookmarks WHERE title='test_title' ''')    
    assert cursor.fetchone()[0] == 1   
    
    #cleanup
    cursor.close()
    database_manager.drop_table("bookmarks")

# CREATE A TEST FOR DELETE
def test_database_manager_delete_bookmark(database_manager):

    # arrange
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    data = {
        "title": "test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    criteria = {
        "title": "test_title"
    }

    # act
    database_manager.add("bookmarks", data)
    database_manager.delete("bookmarks", criteria)

    # assert
    conn = database_manager.connection
    cursor = conn.cursor()
    cursor.execute(''' SELECT * FROM bookmarks WHERE title='test_title' ''')    
    assert cursor.fetchone() == None # this proves that the bookmark called "test_title" no longer exists

    #cleanup
    cursor.close()
    database_manager.drop_table("bookmarks")

# CREATE A TEST FOR SELECT
def test_database_manager_select_bookmark(database_manager):

    # arrange
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    data1 = {
        "title": "1_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    data2 = {
        "title": "2_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    data3 = {
        "title": "3_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    criteria = {
        "title": "2_test_title"
    }

    # act
    database_manager.add("bookmarks", data1)
    database_manager.add("bookmarks", data2)
    database_manager.add("bookmarks", data3)
    cursor = database_manager.select("bookmarks", criteria, None)

    # assert
    assert cursor.fetchone()[0] == 2 # this proves that the bookmark called "2_test_title" is returned

    #cleanup
    cursor.close()
    database_manager.drop_table("bookmarks")

# CREATE A TEST FOR SORT
def test_database_manager_sort_bookmark(database_manager):

    # arrange
    database_manager.create_table(
        "bookmarks",
        {
            "id": "integer primary key autoincrement",
            "title": "text not null",
            "url": "text not null",
            "notes": "text",
            "date_added": "text not null",
        },
    )

    data1 = {
        "title": "1_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    data2 = {
        "title": "2_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    data3 = {
        "title": "3_test_title",
        "url": "http://example.com",
        "notes": "test notes",
        "date_added": datetime.utcnow().isoformat()        
    }

    order_by = "title ASC"

    criteria = {
        "title": "3_test_title"
    }

    # act
    database_manager.add("bookmarks", data3)
    database_manager.add("bookmarks", data2)
    database_manager.add("bookmarks", data1)
    cursor = database_manager.select("bookmarks", None, order_by)
    #cursor = database_manager.select("bookmarks", criteria, None)

    # assert
    returned_list = []
    sorted_list = cursor.fetchall()
    for value in sorted_list:
        returned_list.append(value[0])

    assert returned_list == [3,2,1] # this proves that the bookmarks are sorted by title

    #cleanup
    cursor.close()
    database_manager.drop_table("bookmarks")