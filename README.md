The project is completed as a part of CSC343: Introduction to Databases course offered by the University of Toronto in Fall 2022. The project was done exclusively by Chao Glen Xu and Nehchal Kalsi.

# Embedded SQL Ride Dispatching Application

## Introduction

The world has witnessed a surge in the popularity and usage of ride-dispatching apps. Such platforms require a robust backend that effectively communicates with various stakeholders like drivers, passengers, and dispatchers. This project, the "Embedded SQL Ride Dispatching Application," is envisioned as a vital component of such an application. It synergizes the power of SQL embedded within Python to enhance the performance, scalability, and functionality of these apps.

## 🔍 Objective

Design and develop a system that offers seamless interactions for users of a ride-sharing platform:

- **clock_in**: Allows drivers to indicate the onset of their work shifts.
- **pick_up**: Records the moment a driver confirms picking up a client.
- **dispatch**: Facilitates dispatchers in allocating drivers in response to ride requests within specific geographical confines.

## 🛠 Implementation Details

- **Language**: Python
- **Database**: PostgreSQL
- **Library**: psycopg2

## 📝 Development Guidelines

1. Prioritize the integration of Embedded SQL within Python.
2. Refrain from incorporating direct user or file inputs inside the core methods.
3. The methods, `connect()` and `disconnect()`, serve as boilerplate for database operations. Keep these untouched.
4. Focus all developmental efforts on the `part2.py` file.
5. Modularize code using helper functions to boost readability and maintainability.
6. Within methods, introduce temporary views to compartmentalize tasks. These views should be discarded post-usage.
7. Let docstrings guide the code implementation. Eschew unnecessary additions.
8. For geographical data columns (`geo_loc`), leverage PostgreSQL's point type.
9. For lengthy SQL statements, exploit Python's multi-line strings to ensure neatness and adherence to conventions.

## ⏳ Getting Started

1. Ensure `PostgreSQL` and the `psycopg2` library are installed.
2. Clone the repository.
3. Navigate to the project directory and run `part2.py`.
4. Use the provided functions to test different scenarios and functions.


