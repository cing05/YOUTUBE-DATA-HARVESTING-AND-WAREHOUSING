# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING
YouTube Data Harvesting and Warehousing is a project designed to empower users to access and analyze data from multiple YouTube channels. The project employs SQL, MongoDB, and Streamlit to create a user-friendly application for retrieving, saving, and querying YouTube channel and video data.

Tools and Libraries Used:
Streamlit: Streamlit library is utilized to develop a user-friendly interface, enabling users to interact with the application for data retrieval and analysis operations.

Python: Python serves as the primary programming language for this project. Known for its ease of learning and versatility, Python is used for developing the entire application, including data retrieval, processing, analysis, and visualization.

Google API Client: The googleapiclient library in Python facilitates communication with various Google APIs. In this project, it interacts with YouTube's Data API v3, enabling the retrieval of critical information such as channel details, video specifics, and comments. This allows developers to access and manipulate YouTube's vast data resources through code.

MongoDB: MongoDB, a document database, is used for its scalable architecture and flexibility with evolving data schemas. It simplifies the storage of structured or unstructured data, employing a JSON-like format for storing documents.

MySQL Connector: The mysql-connector library in Python is used to connect and interact with the MySQL database. It provides an interface for executing SQL queries, enabling efficient data migration and manipulation within the MySQL database.

Ethical Considerations for YouTube Data Scraping:
Ethical handling of YouTube content scraping is vital. It requires adherence to YouTube's terms and conditions, obtaining proper authorization, and compliance with data protection regulations. Responsible data handling ensures privacy, confidentiality, and guards against misuse or misrepresentation. Additionally, considering the potential impact on the platform and its community fosters a fair and sustainable scraping process, extracting valuable insights while upholding integrity.

Required Libraries:
googleapiclient.discovery
streamlit
mysql-connector
pymongo
pandas
Features:
The YouTube Data Harvesting and Warehousing application offers the following functionalities:

Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in a MongoDB database, serving as a data lake.
Migration of data from the data lake to a MySQL database for efficient querying and analysis.
Search and retrieval of data from the MySQL database using different search options.
