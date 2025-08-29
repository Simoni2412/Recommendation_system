import json
import re
from typing import Set

def get_first_two_words(ingredient: str) -> str:
    """Get the first two words of an ingredient for duplicate detection."""
    words = ingredient.strip().split()
    if len(words) >= 1:
        return f"{words[0]} {words[1]}".lower()
    elif len(words) == 1:
        return words[0].lower()
    else:
        return ""

def clean_ingredient(ingredient: str) -> str:
    """Clean and normalize ingredient name."""
    # Remove extra spaces and convert to lowercase
    cleaned = re.sub(r'\s+', ' ', ingredient.strip())
    # Remove common prefixes/suffixes and parentheses
    cleaned = re.sub(r'^(extract|oil|powder|acid|filtrate|seed|fruit|leaf|root|flower)\s+', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+(extract|oil|powder|acid|filtrate|seed|fruit|leaf|root|flower)$', '', cleaned, flags=re.IGNORECASE)
    # Remove parentheses and their contents
    cleaned = re.sub(r'\([^)]*\)', '', cleaned)
    # Remove extra spaces again
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_unique_ingredients(json_file_path: str) -> Set[str]:
    """
    Extract all unique ingredients from the JSON file, avoiding duplicates based on first 2 words.
    
    Args:
        json_file_path: Path to the JSON file
    
    Returns:
        Set of unique ingredient names
    """
    # Load the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    unique_ingredients = set()
    first_two_words_seen = set()
    
    # Process each product
    for product in data['products']:
        ingredients = product.get('ingredients', '')
        if ingredients and ingredients.strip():
            # Split ingredients by comma and clean them
            ingredient_list = [ingredient.strip() for ingredient in ingredients.split(',')]
            for ingredient in ingredient_list:
                if ingredient.strip():  # Only add non-empty ingredients
                    first_two_words = get_first_two_words(ingredient)
                    
                    # Only add if we haven't seen these first two words before
                    if first_two_words and first_two_words not in first_two_words_seen:
                        unique_ingredients.add(ingredient.strip())
                        first_two_words_seen.add(first_two_words)
    
    return unique_ingredients

def main():
    """Main function to extract and display unique ingredients."""
    input_file = "output_moida_batched.json"
    
    try:
        unique_ingredients = extract_unique_ingredients(input_file)
        
        # Convert to sorted list for better readability
        sorted_ingredients = sorted(list(unique_ingredients))
        
        print(f"Total unique ingredients found (after removing duplicates based on first 2 words): {len(sorted_ingredients)}")
        print("\nUnique ingredients list:")
        print("=" * 50)
        
        for i, ingredient in enumerate(sorted_ingredients, 1):
            print(f"{i:3d}. {ingredient}")
        
        # Save to a text file for easy reference
        output_file = "unique_ingredients_cleaned.txt"
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(f"Total unique ingredients (after removing duplicates based on first 2 words): {len(sorted_ingredients)}\n")
            file.write("=" * 50 + "\n\n")
            for i, ingredient in enumerate(sorted_ingredients, 1):
                file.write(f"{i:3d}. {ingredient}\n")
        
        print(f"\nCleaned ingredients list saved to: {output_file}")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{input_file}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
