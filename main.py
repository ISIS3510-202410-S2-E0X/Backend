from contextlib import asynccontextmanager
import random
import pytz
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import firebase_admin
from firebase_admin import credentials, firestore

jst = pytz.timezone('America/Bogota')
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    trigger = CronTrigger(hour='*/2', timezone=jst)
    scheduler.add_job(trigger_aggregated_stats_update, trigger)
    scheduler.add_job(trigger_update_categories, trigger)
    scheduler.start()
    yield # here, the app turns on and starts to receive requests
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

cred = credentials.Certificate("foodbook-back-firebase-adminsdk-to94a-90fe879afa.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

# ----------------------------
# API Endpoints
# ----------------------------

@app.get("/recommendation/{uid}")
async def get_recommendation_for_user(uid: str):
    print('INFO: GETTING RECOMMENDATION')
    reviews = await get_all_reviews()

    user_reviews = []
    if reviews:
        for review in reviews:
            try:
                if review['user']['id'] == uid:
                    user_reviews.append(review)
            except KeyError:
                pass
    
    categories = await get_all_categories_by_user(uid)
    # print(user_reviews)

    
    if user_reviews == []:
        raise HTTPException(status_code=404, detail="User has no reviews")
    else:
        selected_category = last_category_from_review(user_reviews[0], categories)

    for_you_spots = []
    while (for_you_spots == []):
        for_you_spots = await restaurants_with_category(selected_category)

    return {"spots": for_you_spots, "category": selected_category, "user": uid}


# ----------------------------
# Trigger updates for aggregated stats
# ----------------------------

@app.get("/trigger_update")
async def trigger_aggregated_stats_update():
    print('INFO: TRIGGERING UPDATE')
    # 1. get all spots
    spots = await get_all_documents_from_collection('spots')

    # 2. get all spot reviews
    for spot_id, spot in spots.items():
        spot_reviews = await get_spot_reviews(spot['reviewData']['userReviews'])
        
        # 3. calculate new stats
        avg_stats = await update_spot_stats(spot['reviewData']['stats'], spot_reviews)

        # 4. update stats in spot document
        await update_spot_stats_firebase(avg_stats, spot_id)

    return {}

# ----------------------------
# Trigger updates for calculating spot categories
# ----------------------------

@app.get("/trigger_update_categories") # Im leaving this as a get request for testing purposes
async def trigger_update_categories():
    print('INFO: TRIGGERING UPDATE CATEGORIES')
    # 1. get all spots
    spots = await get_all_documents_from_collection('spots')

    spots_reviews = {}

    # 2. get all spot reviews
    for spot_id, spot in spots.items():
        spots_reviews[spot_id] = await get_spot_reviews(spot['reviewData']['userReviews'])
    
    # 3. for each spot, read all reviews and sum up the categories
    for spot_id, spot_reviews in spots_reviews.items():
        categories = {}
        for review in spot_reviews:
            for category in review['selectedCategories']:
                if category in categories:
                    categories[category] += 1
                else:
                    categories[category] = 1

        # 4. update the spot document with the categories
        spot_ref = db.collection('spots').document(spot_id)
        spot_ref.update({
            'categories': [{"name": k, "count": v} for k, v in categories.items()]
        })
        
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

async def get_all_categories_by_user(user_id: str):
    collection_ref = db.collection('reviews')
    # get all categories
    reviews = collection_ref.where('user.id', '==', user_id).get()
    categories_list = []
    for review in reviews:
        # the categories are stored inside the review in the field 'selectedCategories'
        review_data = review.to_dict()
        categories_list = review_data['selectedCategories']

    return categories_list

async def get_all_documents_from_collection(collection_name: str):
    collection_ref = db.collection(collection_name)
    docs = collection_ref.stream()
    return_documents = {}
    for doc in docs:
        return_documents[doc.id] = doc.to_dict()
        

    return return_documents


async def update_spot_stats_firebase(avg_stats: dict, spot_id: str):
    spot_ref = db.collection('spots').document(spot_id)
    spot_ref.update({
        'reviewData.stats': avg_stats
    })


def last_category_from_review(review: dict, categories: list):
    selected_category = review['selectedCategories'][-1]
    if not selected_category:
        rand_int = random.randint(0, len(categories))
        return categories[rand_int]['name']
        
    return selected_category


async def get_spot_reviews(review_references: list):
    # references look like this: <google.cloud.firestore_v1.document.DocumentReference object at 0x106f6b490>
    reviews = []
    for review in review_references:
        each_review = review.get()
        reviews.append(each_review.to_dict())
    
    return reviews
    
    
async def update_spot_stats(spot: list, spot_reviews: list):
    avg_stats = {
        'waitTime': 0,
        'foodQuality': 0,
        'cleanliness': 0,
        'service': 0,
    }
    
    total_reviews = len(spot_reviews)
    try:
        for review in spot_reviews:
            avg_stats['waitTime'] += review['ratings']['waitTime'] / total_reviews
            avg_stats['foodQuality'] += review['ratings']['foodQuality'] / total_reviews
            avg_stats['cleanliness'] += review['ratings']['cleanliness'] / total_reviews
            avg_stats['service'] += review['ratings']['service'] / total_reviews
    except Exception as e:
        pass
    
    return avg_stats   
        
    
async def restaurants_with_category(selected_category: str):
    spots_ref = db.collection('spots')
    query = spots_ref.where('categories', 'array_contains', selected_category)
    results = query.stream()

    for_you_spots = []
    for doc in results:
        for_you_spots.append(doc.id)
    
    return for_you_spots
