# Analysis of AirBnB data using Hadoop's MapReduce Technology
## High Performance Computational Infrastructure Project Assessment. 

## Data Preparation and Preprocessing:
Processed a comprehensive dataset from Zenodo containing various Airbnb listing characteristics across European cities.
Successfully merged multiple CSV files, each representing data based on city names and weekdays/weekends, into a unified dataset. This involved creating a new 'City' column and removing unnecessary data, resulting in a streamlined dataset ready for analysis.

## MapReduce Algorithm Design & Implementation:
Designed a multi-stage MapReduce algorithm to efficiently process and analyze the dataset.
In the first job, calculated the average guest satisfaction rating for each city and room type combination. This involved filtering essential features and outputting key-value pairs for further processing.
The second job aggregated the average guest satisfaction score by each city, providing a holistic view of guest experiences in different urban areas.
The final job identified the city with the highest and lowest average guest satisfaction scores, offering actionable insights for stakeholders.

## Results & Evaluation:
Successfully identified Budapest as the city with the highest overall guest satisfaction score of 95.16 and Lisbon with the lowest score of 88.99.
Visualized the results using bar charts, showcasing the satisfaction scores for various room types across different cities. This visual representation allowed for easy comparison and understanding of guest preferences and experiences.

