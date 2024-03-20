import random
# import pytz
from fastapi import FastAPI, HTTPException
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from apscheduler.triggers.cron import CronTrigger
import firebase_admin
from firebase_admin import credentials, firestore


app = FastAPI()

cred = credentials.Certificate("foodbook-back-firebase-adminsdk-to94a-90fe879afa.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

# ----------------------------
# API Endpoints
# ----------------------------

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
    for_you_spots = await restaurants_with_category(selected_category)

    return {"spots": for_you_spots, "category": selected_category, "user": uid}


# ----------------------------
# Trigger updates for aggregated stats
# ----------------------------

# jst = pytz.timezone('America/Bogota')
# scheduler = AsyncIOScheduler()

@app.get("/trigger_update")
async def trigger_aggregated_stats_update():
    # 1. get all spots
    spots = await get_all_spots()
    print(spots)

    # 2. get all spot reviews
    for spot in spots:
        spot_reviews = await get_spot_reviews(spot['userReviews'])
        
        
        # 3. calculate new stats
        # update_spot_stats(spot, spot_reviews)

        # 4. update stats in spot document
        # update_spot_stats(spot, spot_reviews)
        pass

    return {}



# ----------------------------
# Functions 
# ----------------------------


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


async def get_all_spots():
    collection_ref = db.collection('spots')
    # get all spots
    spots = collection_ref.get()
    spots_list = []
    for spot in spots:
        spots_list.append(spot.to_dict())
    
    return spots_list


def last_category_from_review(review: dict, categories: list):
    selected_category = review['selectedCategories'][-1]
    if not selected_category:
        rand_int = random.randint(0, len(categories))
        return categories[rand_int]['name']
        
    return selected_category


async def get_spot_reviews(review_references: list):
    # references look like this:  <google.cloud.firestore_v1.document.DocumentReference object at 0x106f6b490>
    reviews = []
    for review_ref in review_references:
        review = db.document(review_ref).get()
        reviews.append(review.to_dict())
    
    return reviews
    


async def restaurants_with_category(selected_category: str):
    spots_ref = db.collection('spots')
    query = spots_ref.where('categories', 'array_contains', selected_category)
    results = query.stream()

    for_you_spots = []
    for doc in results:
        for_you_spots.append(doc.id)
    
    return for_you_spots
