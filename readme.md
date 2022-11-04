# VRA Cleaner - Videogame Recommender Algorithm
Program designed to clean both the information from games and reviews, with the objective of developing an algorithm able to recommend a videogame to the user based in his likes. It's based on Python 3.9 and works with AWS.

## Table of contents
* [General info] (#general-info)
* [Technologies] (#technologies)
* [Setup] (#setup)

## General info
This project downloads the games file and reviews files stored in S3 bucket.
Done this, first game data is treated, transforming the info from a semi structured data format to a structured one.
Some fields are treated, while others are deleted.
After the games cleaning, it's the turn for reviews, as the objective is getting only information from those users that qualify as "real users".
The meaning of this is that users with only the best review, or the worst, are deleted in order no to get a biased algorithm
After all the info is treated, the different results are stored in a S3 Bucket.

## Technologies
Project is created with:
* Python 3.9
* Pandas 1.4.4
* Scikit-learn 1.1.3
* S3fs 2022.10.0

## Setup
To run this project, you'll need to install the libraries noted in requirements.txt.
This project is made to work inside AWS.
A file named secrets.toml containing the S3 Bucket name isn't uploaded.