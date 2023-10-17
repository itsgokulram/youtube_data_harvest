# youtube_data_harvest

Project Title: YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Skills take away From This Project: Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Managment using MongoDB (Atlas) and SQL.

Domain : Social Media (YouTube)

Problem Statement:

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application has the following features.

1.Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.

2.Automatically store the data in a MongoDB database as a data lake.

3.Ability to collect data for up to 10 different YouTube channels and store them in the data lake

4.Automatically migrate all the data from the data lake to a SQL database as tables.

5.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Obtain API credentials:

1.Go to the Google Developers Console.

2.Create a new project or select an existing project.

3.Enable the YouTube Data API v3.

Configuration:

1.Run the initial create_tables.sql file once to create the tables as needed

2.Open the main.py file in the project directory.

3.Set the desired configuration options

4.Specify your YouTube API key.

5.Choose the database connection details (SQL and MongoDB).

6.Get the Youtube Channel ID from the Youtube's sourcepage

7.provide the Youtube Channel ID in the streamlit page  for data to be harvested.


Usage:

1.Launch the Streamlit app: streamlit run main.py

2.Run the main.py script, make sure you have main and sql files in the same folder.

3.The app will start and open in your browser. You can explore the harvested YouTube data and visualize the results.
