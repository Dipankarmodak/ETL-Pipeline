# ETL-Pipeline and Sales Analysis Dashboard of Amazon Dataset ![icon](https://bit.ly/3A6I0ux)
## **Ineuron Business Analytics Internship project to perform Extract transform and Load operations and make a Dashboard of the same**

## Problem Statement
Sales management has gained importance to meet increasing competition and the needfor improved methods of distribution to reduce cost and to increase profits. Salesmanagement today is the most important function in a commercial and business enterprise.Do ETL : Extract-Transform-Load some Amazon dataset and find  Sales-trend -> month wise , year wise , yearly_month wise Find key metrics and factors and show the meaningful relationships between attributes.

Dataset:You can find the dataset on the given link : https://drive.google.com/drive/folders/1FkmFVL8wlJmQWP1z52TD8PlhOJhitTyI?usp=sharing </br> 
* SALESDATA.xls
* REGION.xls
* CUSTOMER.xls
* CUSTOMERADDRESS.xls
* DIVISION.xls

**Only GPS.xls was not initialy present therefore we had to geocode the text address into latitude and longitude using an external API.**

🛠️ Approaches and Tools </br>

* AWS S3 bucket storage
* Python ( Pandas,Numpy,Feature-Engine,Pylint,Pytest)
* AWS glue Crawler
* AWS Athena
* Tableau Server

The following KPI were analyzed </br>
* Sales quantity by month and year 
* Revenue trend by month and year 
* Profit margin by month and year 
* Cost of goods sold by the month and year 
* Top 10 products by profit margin, revenue for each customer segment

## The ETL Pipeline in Action
![bandicam 2022-01-18 04-05-41-564](https://user-images.githubusercontent.com/77185203/149845626-321a35c4-5ca8-4b61-b0fc-d7f9a92e0097.gif) </br>

## Tableau Server Dashboard
![bandicam 2022-01-18 05-04-24-743](https://user-images.githubusercontent.com/77185203/149848075-f1ba30fd-c6be-4638-8665-feaa3d117931.gif)

## For the full video :- https://youtu.be/h6I_FaoWhhI


## Workflow of the Project
![image](https://user-images.githubusercontent.com/77185203/149848749-dc168ace-1f58-4517-bba3-169c89f5cec2.png) </br>

## Documentation Links-
All the nesscary documentations are present in the main branch.

## Tableau Dashboard links-
* Tableau  Server-https://tabsoft.co/3GPx9re  
* Tableau Public-https://tabsoft.co/3nADP5c , https://tabsoft.co/3A6uU0e ,https://tabsoft.co/3FxfK5f

