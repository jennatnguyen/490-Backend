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

# - rent a film out to a customer

#--------------------CUSTOMER PAGE--------------------------------

# - view a list of all customers (Pref. using pagination)
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

# -  the ability to filter/search customers by their customer id, first name or last name.
# -  add a new customer
# -  edit a customer’s details
# -  delete a customer if they no longer wish to patron at store
# -  view customer details and see their past and present rental history
# - indicate that a customer has returned a rented movie
#------------------------------------------------------------------
if __name__=="__main__":
    print("connecting to DB...")
    app.run(debug=True)