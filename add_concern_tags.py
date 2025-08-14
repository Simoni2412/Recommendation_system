import json
import re
from typing import List, Dict, Set
from skincare_ingredients import SKINCARE_INGREDIENTS

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

def find_matching_concerns(ingredients: str, skincare_ingredients: Dict[str, List[str]]) -> Set[str]:
    """
    Find matching concerns based on ingredients.
    
    Args:
        ingredients: Comma-separated ingredients string
        skincare_ingredients: Dictionary of concerns and their associated ingredients
    
    Returns:
        Set of matching concern tags
    """
    if not ingredients or ingredients.strip() == "":
        return set()
    
    # Split ingredients by comma and clean them
    ingredient_list = [ingredient.strip() for ingredient in ingredients.split(',')]
    normalized_ingredients = [normalize_ingredient(ingredient) for ingredient in ingredient_list]
    
    matching_concerns = set()
    
    for concern, concern_ingredients in skincare_ingredients.items():
        for concern_ingredient in concern_ingredients:
            normalized_concern_ingredient = normalize_ingredient(concern_ingredient)
            
            # Check for exact matches or partial matches
            for normalized_ingredient in normalized_ingredients:
                # Check for exact match
                if normalized_concern_ingredient == normalized_ingredient:
                    matching_concerns.add(concern)
                    break
                
                # Check for partial matches (ingredient contains concern ingredient or vice versa)
                if (normalized_concern_ingredient in normalized_ingredient or 
                    normalized_ingredient in normalized_concern_ingredient):
                    # Additional check to avoid false positives
                    if len(normalized_concern_ingredient) > 3 and len(normalized_ingredient) > 3:
                        matching_concerns.add(concern)
                        break
    
    return matching_concerns

def add_concern_tags_to_products(json_file_path: str, output_file_path: str = None):
    """
    Add concern tags to products based on their ingredients.
    
    Args:
        json_file_path: Path to the input JSON file
        output_file_path: Path to the output JSON file (optional, defaults to input file)
    """
    # Load the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # Process each product
    total_products = len(data['products'])
    products_with_concerns = 0
    
    for i, product in enumerate(data['products'], 1):
        ingredients = product.get('ingredients', '')
        matching_concerns = find_matching_concerns(ingredients, SKINCARE_INGREDIENTS)
        
        # Add concern tags to the product
        product['concern_tags'] = list(matching_concerns)
        
        # Print progress and info for debugging
        if matching_concerns:
            products_with_concerns += 1
            print(f"[{i}/{total_products}] Product: {product['name']}")
            print(f"Concerns: {', '.join(matching_concerns)}")
            print("-" * 50)
        else:
            print(f"[{i}/{total_products}] Product: {product['name']} - No concerns matched")
    
    # Save the updated JSON file
    output_path = output_file_path or json_file_path
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    
    print(f"\nUpdated JSON file saved to: {output_path}")
    print(f"Total products processed: {total_products}")
    print(f"Products with concern tags: {products_with_concerns}")
    print(f"Products without concern tags: {total_products - products_with_concerns}")

def main():
    """Main function to run the script."""
    input_file = "output_moida_batched.json"
    output_file = "output_moida_batched_with_concerns.json"
    
    try:
        add_concern_tags_to_products(input_file, output_file)
        print("Successfully added concern tags to all products!")
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{input_file}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 