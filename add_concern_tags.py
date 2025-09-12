import json
import re
from typing import List, Dict, Tuple
from skincare_ingredients import RANKED_SKINCARE_INGREDIENTS

def load_ranked_ingredients(file_path: str = "unique_ingredients_cleaned.txt") -> List[str]:
    """Load the ranked ingredients list from the text file."""
    ranked_ingredients = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('=') and not line.startswith('Total'):
                    # Extract ingredient name from numbered line (e.g., "1. Sodium Hyaluronate")
                    if '. ' in line:
                        ingredient = line.split('. ', 1)[1]
                        ranked_ingredients.append(ingredient.strip())
        print(f"Loaded {len(ranked_ingredients)} ranked ingredients")
    except FileNotFoundError:
        print(f"Warning: {file_path} not found. Using default ingredient ranking.")
        # Fallback to default ranking if file not found
        ranked_ingredients = []

    return ranked_ingredients

def normalize_ingredient(ingredient: str) -> str:
    """Normalize ingredient name for better matching."""
    # Remove extra spaces and convert to lowercase
    normalized = re.sub(r'\s+', ' ', ingredient.strip()).lower()
    # Remove common prefixes/suffixes and parentheses
    normalized = re.sub(r'^(extract|oil|powder|acid|filtrate|seed|fruit|leaf|root|flower)\s+', '', normalized)
    normalized = re.sub(r'\s+(extract|oil|powder|acid|filtrate|seed|fruit|leaf|root|flower)$', '', normalized)
    # Remove parentheses and their contents
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    # Remove extra spaces again
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def find_matching_concerns_with_ranking(ingredients: str, skincare_ingredients: Dict[str, List[str]], ranked_ingredients: List[str]) -> List[Tuple[str, int]]:
    """
    Find matching concerns based on ingredients and rank them by ingredient priority.
    
    Args:
        ingredients: Comma-separated ingredients string
        skincare_ingredients: Dictionary of concerns and their associated ingredients
        ranked_ingredients: List of ingredients in ranked order (first = highest priority)

    Returns:
        List of tuples (concern, priority_score) sorted by priority
    """
    if not ingredients or ingredients.strip() == "":
        return []
    
    # Split ingredients by comma and clean them
    ingredient_list = [ingredient.strip() for ingredient in ingredients.split(',')]
    normalized_ingredients = [normalize_ingredient(ingredient) for ingredient in ingredient_list]
    
    concern_scores = {}  # concern -> best_priority_score
    
    for concern, concern_ingredients in skincare_ingredients.items():
        best_score = float('inf')
        for concern_ingredient in concern_ingredients:
            normalized_concern_ingredient = normalize_ingredient(concern_ingredient)
            
            # Check for exact matches or partial matches
            for normalized_ingredient in normalized_ingredients:
                # Check for exact match
                if normalized_concern_ingredient == normalized_ingredient:
                    # Find the rank of this ingredient
                    for i, ranked_ingredient in enumerate(ranked_ingredients):
                        if normalize_ingredient(ranked_ingredient) == normalized_ingredient:
                            best_score = min(best_score, i)
                            break
                    break
                
                # Check for partial matches
                if (normalized_concern_ingredient in normalized_ingredient or 
                    normalized_ingredient in normalized_concern_ingredient):
                    # Additional check to avoid false positives
                    if len(normalized_concern_ingredient) > 3 and len(normalized_ingredient) > 3:
                        # Find the rank of this ingredient
                        for i, ranked_ingredient in enumerate(ranked_ingredients):
                            if normalize_ingredient(ranked_ingredient) == normalized_ingredient:
                                best_score = min(best_score, i)
                                break
                        break

        # If we found a match, store the best score
        if best_score != float('inf'):
            concern_scores[concern] = best_score

    # Sort concerns by priority score (lower score = higher priority)
    ranked_concerns = sorted(concern_scores.items(), key=lambda x: x[1])

    # Return just the concern names in ranked order
    return [concern for concern, score in ranked_concerns]

def add_concern_tags_to_products(json_file_path: str, output_file_path: str = None):
    """
    Add concern tags to products based on their ingredients, ranked by ingredient priority.
    
    Args:
        json_file_path: Path to the input JSON file
        output_file_path: Path to the output JSON file (optional, defaults to input file)
    """
    # Load the ranked ingredients list
    ranked_ingredients = load_ranked_ingredients()

    # Load the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # Process each product
    total_products = len(data['products'])
    products_with_concerns = 0
    
    for i, product in enumerate(data['products'], 1):
        ingredients = product.get('ingredients', '')
        matching_concerns = find_matching_concerns_with_ranking(ingredients, RANKED_SKINCARE_INGREDIENTS, ranked_ingredients)
        
        # If no concerns detected, default to 'general'
        if not matching_concerns:
            matching_concerns = ["general"]
        else:
            products_with_concerns += 1

        # Add concern tags to the product (already ranked by priority)
        product['concern_tags'] = matching_concerns
        
        # Print progress and info for debugging
        if matching_concerns and matching_concerns != ["general"]:
            print(f"[{i}/{total_products}] Product: {product['name']}")
            print(f"Ranked Concerns: {', '.join(matching_concerns)}")
            print("-" * 50)
        else:
            print(f"[{i}/{total_products}] Product: {product['name']} - No specific concerns matched (tagged as 'general')")
    
    # Save the updated JSON file
    output_path = output_file_path or json_file_path
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    
    print(f"\nUpdated JSON file saved to: {output_path}")
    print(f"Total products processed: {total_products}")
    print(f"Products with specific concern tags: {products_with_concerns}")
    print(f"Products tagged as 'general': {total_products - products_with_concerns}")

def main():
    """Main function to run the script."""
    input_file = "output_moida_batched.json"
    output_file = "new_with_concerns.json"
    
    try:
        add_concern_tags_to_products(input_file, output_file)
        print("Successfully added ranked concern tags to all products!")
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{input_file}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 