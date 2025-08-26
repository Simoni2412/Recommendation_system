# Recommendation_system
Design a recommendation system with ingredient level matching and scoring method


# 1. Webscraped the sample data from Moida with fields 
Scrapped 183 products from different URLS of Moida. Th data was messy but tried to cleanse it. Ingredients list was merged with the description in the Moida's website. 
# 2. Add the concern field or generate it with AI 
Created a generic list of ingredients for each concern in the most effective to least order and fed it to the add_concerns.py script which added the concern tags to the scrapped JSON file and created a new file along wiht concerns tag.
# 3. Design the recommendation system (Cosine similarity)
pending for ingredients level matching 
# 4. Identify the active ingredients 
I have created a list of almost 1715 ingredients fetched from 183 products of Moida and aiming to find the active ones form there.
# 5. Extract the percentage or concerntration of the active ingredients 
# 6. Design the ranking system and recommend
Ranked the most effective ingredients to least in the skincare_ingredients file for each concern 
# 7. Try AI wrapper for recommendation system 
Generated the Open AI Api and currently working on the prompting and displaying the top 5 products related to the user profile concerns and skin typpe as well budget.
# 8. Flag the active ingredients that can create irritation
# 9. Try to add the ambient features 

