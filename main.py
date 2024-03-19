from fastapi import FastAPI, HTTPException
import firebase_admin
from firebase_admin import credentials, firestore
import random

# Inicializa tu aplicación FastAPI
app = FastAPI()

# Inicializa Firebase Admin
cred = credentials.Certificate("foodbook-back-firebase-adminsdk-to94a-90fe879afa.json")
firebase_admin.initialize_app(cred)

# Obtén una referencia a la base de datos de Firestore
db = firestore.client()

@app.get("/recommendation/{uid}")
async def get_recommendation_for_user(uid: str):
    # get all reviews and categories
    reviews = await get_all_reviews()
    categories = await get_all_categories()
    
    # search last user review
    user_reviews = []
    for review in reviews:
        try:
            if review['user'] == uid:
                user_reviews.append(review)
        except KeyError:
            pass
        
    if user_reviews == []:
        selected_category = categories[random.randint(0, len(categories))]['name']
    else:
        selected_category = last_category_from_review(user_reviews[0], categories)

    # then return all restaurants with that category
    for_you_spots = await restaurants_with_category("healthy")

    return {"spots": for_you_spots, "category": selected_category, "user": uid}


async def get_all_reviews():
    collection_ref = db.collection('reviews')
    # get all reviews
    reviews = collection_ref.get()
    reviews_list = []
    for review in reviews:
        reviews_list.append(review.to_dict())
    return reviews_list
    

async def get_all_categories():
    collection_ref = db.collection('categories')
    # get all categories
    categories = collection_ref.get()
    categories_list = []
    for category in categories:
        categories_list.append(category.to_dict())
    
    return categories_list


def last_category_from_review(review: dict, categories: list):
    selected_category = review['selectedCategories'][-1]
    if not selected_category:
        rand_int = random.randint(0, len(categories))
        return categories[rand_int]['name']
        
    return selected_category


async def restaurants_with_category(selected_category: str):
    spots_ref = db.collection('spots')
    query = spots_ref.where('categories', 'array_contains', selected_category)
    results = query.stream()

    for_you_spots = []
    for doc in results:
        for_you_spots.append(doc.id)
    
    return for_you_spots


