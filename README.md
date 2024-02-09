# Project 3: STEDI Human Balance Analytics 
--------------

## Overview
In this Project I aim to leverage the capabilities of Spark and AWS GLue to build a comprehensive data solution. This solution will facilitate the training of a machine learning model using data collected from the STEDI Step Trainers and their companion mobile app. As a data engineer on this project, the focus is on crafting a curated data lakehouse on AWS to support the development of an accurate real-time step-detection machine learning algorithm. 

## Introduction
The STEDI team has developed a cutting-edge STEDI Step Trainer equipped with sensors to aid users in balance exercises. Paired with a mobile app, this device not only records user interactions but also collects valuable sensor data for training a machine learning model. The project's core objective is to transform this diverse set of data into a unified and curated format, enabling seamless training for the machine learning algorithm.

## Project Context

### STEDI Step Trainer Features:
- Designed to train users in balance exercises.
- Equipped with sensors for collecting date from a machine learning model training.
- Mobile app collects user data and interfaces with device sensors.

### Data Collection:
- Step Trainer records object detection distances through motion sensors.
- Mobile app uses the mobile phone accelerometer to detect motion in the X, Y, and Z directions.

### User Participation:
- Millions of early adopters expressing intrest in purchasing and using the STEDI Step accelerometer data for research.

## Project Goals
1. **Data Extraction:** Retrieve data from the sensors of the STEDI Step Trainer and the accompanying mobile app.
2. **Data Curation:** Transform and curate the extracted data into a structured format suitable for a data lakehouse on AWS.
3. **Privacy Considerations:** Ensure that only data from consenting customers is utillized in the training of the machine learning model, respecting privacy concerns.

## Project Summary

As a key player in the STEDI Step Trainer team, our mission is to engineer a data solution that empowers Data Scientist to train a highly accurate machine learning model. By harnessing the capabillities of Spark and AWS Glue, we aspire to provide a robust foundation for advancing human balance analytics, ultimately enhancing the functionality of the STEDI Step Trainer and its companion app.