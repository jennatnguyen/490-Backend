from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector 

app = Flask(__name__)
CORS(app)
#connect to sakil database

# Connect to the database only once when the app starts
def connect_to_db():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='toor',
        database='sakila'
    )


#---------------------LANDING PAGE FEATURES----------------

# - view top 5 rented films of all times
@app.route('/top5-rented-films', methods=['GET'])
def top_5_rented_films():
    con = connect_to_db()
    cursor = con.cursor()
    query = """
    select film.film_id, film.title, category.name, COUNT(rental.inventory_id) as rented 
    from film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN inventory ON film.film_id = inventory.film_id 
    JOIN rental ON rental.inventory_id = inventory.inventory_id
    GROUP BY film.film_id, film.title, category.name
    ORDER BY rented DESC
    LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    #don't display id
    films = [{"id": result[0], "title": result[1], "rented": result[3]} for result in results]

    return jsonify({'films': films})

# - click on any of the top 5 films and view its details
@app.route('/top5-rented-details', methods=['GET'])
def top_5_rented_details():
    con = connect_to_db()

    film_id = request.args.get('id')  # Get the 'id' query parameter
    
    cursor = con.cursor()
    
    if film_id:  # If 'id' is provided, return details for that specific film
        query = """
        select film.film_id, film.title, film.description, film.release_year, film.rating,
        category.name, COUNT(rental.inventory_id) as rented 
        from film
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON category.category_id = film_category.category_id
        JOIN inventory ON film.film_id = inventory.film_id 
        JOIN rental ON rental.inventory_id = inventory.inventory_id
        WHERE film.film_id = %s
        GROUP BY film.film_id, film.title, film.description, film.release_year, film.rating, category.name
        """
        cursor.execute(query, (film_id,))
    else:  # If 'id' is not provided, return top 5 films
        query = """
        select film.film_id, film.title, film.description, film.release_year, film.rating,
        category.name, COUNT(rental.inventory_id) as rented 
        from film
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON category.category_id = film_category.category_id
        JOIN inventory ON film.film_id = inventory.film_id 
        JOIN rental ON rental.inventory_id = inventory.inventory_id
        GROUP BY film.film_id, film.title, film.description, film.release_year, film.rating, category.name
        ORDER BY rented DESC
        LIMIT 5;
        """
        cursor.execute(query)
    
    results = cursor.fetchall()
    cursor.close()

    details = [{"id": result[0], "title": result[1], "genre": result[5], "description": result[2],
                "release_year": result[3], "rating": result[4], "rented": result[6]} 
               for result in results]

    return jsonify({'film_details': details})

# - view top 5 actors part of films I have in the store
@app.route('/top5-actors', methods=['GET'])
def top_5_actors():
    con = connect_to_db()
    cursor = con.cursor()
    query = """
    SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(*) as movies
    FROM actor
    JOIN film_actor ON film_actor.actor_id = actor.actor_id
    JOIN film ON film_actor.film_id = film.film_id
    GROUP BY actor.actor_id
    ORDER BY COUNT(*) DESC
    LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    #don't display id
    actors = [{"id": result[0], "name": result[1] + " " + result[2], "movies": result[3]} for result in results]

    return jsonify({'actors': actors})

# - view actor’s details and view their top 5 rented films
@app.route('/top5-actors-films', methods=['GET'])
def top_5_actors_films():
    con = connect_to_db()
    actor_id = request.args.get('actor_id')

    if not actor_id:
        return jsonify({"error": "actor_id parameter is required"}), 400
    
    cursor = con.cursor()

    query = """
    SELECT film.film_id, film.title, COUNT(rental.inventory_id) AS rented
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_actor.film_id = film.film_id
    JOIN inventory ON film.film_id = inventory.film_id
    JOIN rental ON rental.inventory_id = inventory.inventory_id
    WHERE film_actor.actor_id = %s
    GROUP BY film.film_id, film.title
    ORDER BY rented DESC
    LIMIT 5;
    """
    cursor.execute(query, (actor_id,))
    results = cursor.fetchall()
    cursor.close()

    films = [{"film_id": result[0], "movie": result[1], "rentals": result[2]} for result in results]

    return jsonify({'films': films})

#--------------------FILMS PAGE FEATURES-----------------

#display all the films
@app.route('/getTable', methods=['GET'])
def get_table():
    con = connect_to_db()
    cursor = con.cursor()
    
    query = """
    SELECT film.film_id, film.title, category.name AS genre,
       GROUP_CONCAT(CONCAT(actor.first_name, ' ', actor.last_name) SEPARATOR ', ') AS actors
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_actor.film_id = film.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id
    GROUP BY film.film_id, film.title, category.name;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    table = [{"id": result[0], "title": result[1], "genre": result[2], "actors": result[3]} 
             for result in results]

    return jsonify({"films": table})

# - search film by name of film, actor name, or film genre 
# i didn't even use these apis for my search i actually just
# did it using the front end code but i wrote it before so
# i might as well keep it in

#get film by film name
@app.route('/get-film-by-name', methods=['GET'])
def get_film_by_name():
    con = connect_to_db()

    film_name = request.args.get('film_name')

    if not film_name:
        return jsonify({"error": "parameter is required"}), 400
    
    cursor = con.cursor()
    query = """
    SELECT film.film_id, film.title, category.name AS genre,
       GROUP_CONCAT(CONCAT(actor.first_name, ' ', actor.last_name) SEPARATOR ', ') AS actors
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_actor.film_id = film.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id
    WHERE film.title LIKE %s
    GROUP BY film.film_id, film.title, category.name;
    """
    cursor.execute(query,(f"%{film_name}%",))
    results = cursor.fetchall()
    cursor.close()
    table = [{"id": result[0], "title": result[1], "genre": result[2], "actors": result[3]} 
                for result in results]
     #you may want to parse
    return jsonify({"film": table})

#search by actor name
@app.route('/get-film-by-actor', methods=['GET'])
def get_film_by_actor():
    con = connect_to_db()

    actor_name = request.args.get('actor_name')

    if not actor_name:
        return jsonify({"error": "parameter is required"}), 400
    
    cursor = con.cursor()
    query = """
    SELECT film.film_id, film.title, category.name AS genre,
       GROUP_CONCAT(CONCAT(actor.first_name, ' ', actor.last_name) SEPARATOR ', ') AS actors
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_actor.film_id = film.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id
    WHERE actor.first_name LIKE %s OR actor.last_name LIKE %s
    GROUP BY film.film_id, film.title, category.name;
    """
    cursor.execute(query,(f"{actor_name}%", f"%{actor_name}%"))
    results = cursor.fetchall()
    cursor.close()
    table = [{"id": result[0], "title": result[1], "genre": result[2], "actors": result[3]} 
                for result in results]
     #you may want to parse
    return jsonify({"film": table})

#get film by genre
@app.route('/get-film-by-genre', methods=['GET'])
def get_film_by_genre():
    con = connect_to_db()

    genre = request.args.get('genre')

    if not genre:
        return jsonify({"error": "parameter is required"}), 400
    
    cursor = con.cursor()
    
    query = """
    SELECT film.film_id, film.title, category.name AS genre,
       GROUP_CONCAT(CONCAT(actor.first_name, ' ', actor.last_name) SEPARATOR ', ') AS actors
    FROM film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN film_actor ON film_actor.film_id = film.film_id
    JOIN actor ON film_actor.actor_id = actor.actor_id
    WHERE category.name LIKE %s
    GROUP BY film.film_id, film.title, category.name;
    """
    cursor.execute(query,(f"{genre}%",))
    results = cursor.fetchall()
    cursor.close()
    table = [{"id": result[0], "title": result[1], "genre": result[2], "actors": result[3]} 
             for result in results]
     #you may want to parse
    return jsonify({"film": table})

# - view details of the film
@app.route('/film-details', methods=['GET'])
def film_details():
    con = connect_to_db()

    film_id = request.args.get('id')  # Get the 'id' query parameter
    
    cursor = con.cursor()
    
    query = """
    select film.film_id, film.title, film.description, film.release_year, film.rating,
    category.name, film.rental_rate, film.length, film.replacement_cost, film.rental_duration
    from film
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON category.category_id = film_category.category_id
    JOIN inventory ON film.film_id = inventory.film_id 
    JOIN rental ON rental.inventory_id = inventory.inventory_id
    WHERE film.film_id = %s
    GROUP BY film.film_id, film.title, film.description, film.release_year, film.rating, category.name,
    film.rental_rate, film.length, film.replacement_cost, film.rental_duration;
    """
    cursor.execute(query, (film_id,))
    
    results = cursor.fetchall()
    cursor.close()

    details = [{"id": result[0], "title": result[1], "genre": result[5], "description": result[2],
                "release_year": result[3], "rating": result[4], "rental_rate": "$" + str(result[6]),
                "length": str(result[7]) + " minutes", "replacement_cost": "$" + str(result[8]),
                "duration": str(result[9]) + " days"} 
               for result in results]

    return jsonify({'film_details': details})

# [X] rent a film out to a customer
@app.route('/rentFilm', methods=['POST'])
def rent_film():
    con = connect_to_db()
    cursor = con.cursor()

    data = request.json
    customer_id = data.get('customer_id')
    film_id = data.get('film_id')

    if not customer_id or not film_id:
        return jsonify({"error": "Missing customer ID or film ID"}), 400
    
    try:
        query = """
        INSERT INTO rental (inventory_id, customer_id, staff_id, rental_date)
        SELECT i.inventory_id, %s, 1, NOW()
        FROM inventory i
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE f.film_id = %s 
        LIMIT 1;
        """
        cursor.execute(query, (customer_id, film_id))
        con.commit()

        cursor.close()
        return jsonify({"message": "Movie rented successfully!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#--------------------CUSTOMER PAGE--------------------------------

# [X] view a list of all customers (Pref. using pagination)
@app.route('/customerList', methods=['GET'])
def customer_list():
    con = connect_to_db()
    cursor = con.cursor()

    query = """
    select customer_id, first_name, last_name, email from customer;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    table = [{"id": result[0], "first_name": result[1], "last_name": result[2], "email": result[3]} 
             for result in results]

    return jsonify({"customers": table})

# [X]  the ability to filter/search customers by their customer id, first name or last name. 

# [X]  add a new customer
@app.route('/addCustomer', methods=['POST'])
def add_customer():
    con = connect_to_db()
    cursor = con.cursor()

    data = request.json
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')

    if not first_name or not last_name or not email:
        return jsonify({"error": "Missing name or email"}), 400
    
    try:
        query = """
        insert into customer (store_id, first_name, last_name, email, address_id) values (1, %s, %s, %s, 1);
        """
        cursor.execute(query, (first_name, last_name, email))
        con.commit()
        cursor.close()
        return jsonify({"message": "Customer added successfully!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# [X]  edit a customer’s details
@app.route('/editCustomer/<int:customer_id>', methods=['PUT'])
def edit_customer(customer_id):
    con = connect_to_db()
    cursor = con.cursor()

    data = request.json
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')

    if not first_name or not last_name or not email:
        return jsonify({"error": "Missing name or email"}), 400
    
    try:
        query = """
        UPDATE customer SET first_name=%s, last_name=%s, email=%s WHERE customer_id=%s;
        """
        cursor.execute(query, (first_name, last_name, email, customer_id))
        con.commit()
        cursor.close()
        return jsonify({"message": "Customer updated successfully!"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# [x]  delete a customer if they no longer wish to patron at store
@app.route('/deleteCustomer/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    con = connect_to_db()
    cursor = con.cursor()

    try:
        query = """
        DELETE FROM customer WHERE customer_id=%s;
        """
        cursor.execute(query, (customer_id,))
        con.commit()
        cursor.close()
        return jsonify({"message": "Customer deleted successfully!"})
    
    except Exception as e:
        print(f"Error deleting customer: {str(e)}")
        return jsonify({"error": str(e)}), 500

# [X]  view customer details and see their past and present rental history
#customer details
@app.route('/customerDetails', methods=['GET'])
def view_customer():
    con = connect_to_db()

    customer_id = request.args.get('id')  # Get the 'id' query parameter
    
    cursor = con.cursor()
    
    query = """
    SELECT first_name, last_name, email, create_date
    FROM customer
    WHERE customer_id = %s;
    """
    cursor.execute(query, (customer_id,))
    
    results = cursor.fetchall()
    cursor.close()

    details = [{"first_name": result[0], "last_name": result[1], "email": result[2],
                "create_date": result[3]} 
               for result in results]

    return jsonify({'customer_details': details})

#---
@app.route('/customerDetails2', methods=['GET'])
def view_customer2():
    con = connect_to_db()

    customer_id = request.args.get('id')  # Get the 'id' query parameter
    
    cursor = con.cursor()
    
    query = """
    SELECT first_name, last_name, email, create_date
    FROM customer
    WHERE customer_id = %s;
    """
    cursor.execute(query, (customer_id,))
    
    results = cursor.fetchall()
    cursor.close()

    details = [{"first_name": result[0], "last_name": result[1], "email": result[2],
                "create_date": result[3]} 
               for result in results]

    return jsonify({'customer_details': details})

#rental history
@app.route('/viewRented', methods=['GET'])
def view_rented():
    con = connect_to_db()

    customer_id = request.args.get('id')  # Get the 'id' query parameter
    
    cursor = con.cursor()
    
    query = """
    SELECT rental.rental_id, film.title, rental.inventory_id, rental.rental_date, 
    rental.return_date
    FROM rental
    JOIN inventory ON inventory.inventory_id = rental.inventory_id
    JOIN film ON film.film_id = inventory.film_id
    WHERE rental.customer_id = %s
    GROUP BY rental.rental_id, film.title, rental.inventory_id, rental.rental_date, 
    rental.return_date;
    """
    cursor.execute(query, (customer_id,))
    
    results = cursor.fetchall()
    cursor.close()

    details = [{"rental_id": result[0], "title": result[1], "inventory_id": result[2],
                "rental_date": result[3], "return_date": result[4]} 
               for result in results]

    return jsonify({'rental_details': details})

# - indicate that a customer has returned a rented movie
#return
@app.route('/returnFilm/<int:rental_id>/<int:customer_id>', methods=['PUT'])
def return_film(rental_id, customer_id):
    con = connect_to_db()
    cursor = con.cursor()

    try:
        query = """
        UPDATE rental
        SET return_date = NOW()
        WHERE rental_id = %s
        AND customer_id = %s
        AND return_date IS NULL;
        """
        cursor.execute(query, (rental_id, customer_id))
        con.commit()

        if cursor.rowcount > 0:
            return jsonify({"message": "Film returned successfully!"})
        else:
            return jsonify({"error": "No current rental found for the given customer and film."}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


#------------------------------------------------------------------
if __name__=="__main__":
    print("connecting to DB...")
    app.run(debug=True)