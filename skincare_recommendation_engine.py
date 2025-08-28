import openai  # or your preferred LLM wrapper
from typing import List, Dict, Tuple, Union
import os
import json
from dotenv import load_dotenv
#import math

# Example of correct OpenAI API usage:
# client = openai.OpenAI(api_key="your-api-key")
# response = client.chat.completions.create(
#   model="gpt-4o-mini",
#   messages=[{"role": "user", "content": "write a haiku about ai"}]
# )
# print(response.choices[0].message.content

class SkincareRecommendationEngine:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        self.products = self.load_products()
        
        if not self.products:
            print("Warning: No products loaded. Please check your data files.")
        else:
            print(f"Successfully loaded {len(self.products)} products")
    
    def load_products(self) -> List[Dict]:
        """Load products from your JSON file with concern tags"""
        try:
            with open("output_moida_batched_with_concerns.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            return data["products"]
        except FileNotFoundError:
            print("Warning: output_moida_batched_with_concerns.json not found. Trying alternative file...")
            try:
                with open("output_moida_batched.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data["products"]
            except FileNotFoundError:
                print("Error: No product data file found. Please ensure you have run the concern tagging script first.")
                return []
        except UnicodeDecodeError:
            print("Error: Unicode decoding issue. Trying with different encoding...")
            try:
                with open("output_moida_batched_with_concerns.json", "r", encoding="latin-1") as f:
                    data = json.load(f)
                return data["products"]
            except Exception as e:
                print(f"Error reading file: {e}")
                return []
    
    def get_recommendations(self, user_concerns: Dict[str, float], skin_type: Union[str, None] = None, 
                           budget: Union[str, None] = None, num_recommendations: int = 5) -> List[Dict]:
        """
        Get personalized product recommendations using LLM with minimal tokens
        
        Args:
            user_concerns: Dict where keys are concerns and values are percentages (0-100)
            skin_type: User's skin type
            budget: Budget constraint
            num_recommendations: Number of products to recommend
        """

        # Get top-scoring products first
        scored_products = self.filter_products_by_concerns(user_concerns, min_score_threshold=5.0)
        if not scored_products:
            print(f"No products found matching concerns: {user_concerns}")
            #print("Available concern tags in products:")
            all_concerns = set()
            for product in self.products[:10]:  # Check first 10 products
                concerns = product.get("concern_tags", [])
                all_concerns.update(concerns)
            print(f"Sample concerns: {list(all_concerns)[:20]}")
            return []

        print(f"Found {len(scored_products)} products with scores >= 5.0")
        print(f"Top 5 scored products:")
        for i, (product, score) in enumerate(scored_products[:5]):
            print(f"  {i+1}. {product['name'][:50]} - Score: {score:.1f} - Concerns: {product.get('concern_tags', [])}")

        top_products = scored_products[:50]

        # Create minimal prompt for LLM
        prompt = self.create_minimal_prompt(
            user_concerns, top_products, skin_type, budget, num_recommendations
        )
        
        print(f"\nSending prompt to LLM (length: {len(prompt)} chars)...")
        print(f"Prompt preview: {prompt[:200]}...")

        # Call the LLM with minimal tokens
        try:
            print(f"Calling OpenAI API with model: {self.model}")
            print(f"Prompt length: {len(prompt)} characters")
            
            response = self.client.responses.create(
                model=self.model,
                #reasoning ={"effort": "low"},
                instructions="You are a skincare expert.",
                input=prompt,
                #temperature=0.0,  # deterministic output
                #max_tokens=1200,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "skincare_recommendations",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "recommendations": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "reason": {"type": "string"},
                                            "priority": {"type": "string"}
                                        },
                                        "required": ["name", "reason", "priority"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["recommendations"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
            
            # print(f"API Response received: {response}")
            # print(f"Response type: {type(response)}")
            # print(f"Response choices: {response.choices}")
            print(response.output_text)

            if response:
                return self.parse_llm_response(response.output_text)
            else:
                print(f"No choices in response: {response}")
                return [{"error": "No response content received"}]

        except Exception as e:
            print(f"API call error: {e}")
            print(f"Error type: {type(e)}")
            return [{"error": f"API call failed: {str(e)}"}]
    
    def normalize_concerns(self, concerns: Dict[str, float]) -> Dict[str, float]:
        """Normalize concern percentages to ensure they sum to 100%"""
        total = sum(concerns.values())
        if total == 0:
            return concerns
        
        normalized = {}
        for concern, percentage in concerns.items():
            normalized[concern] = (percentage / total) * 100
        
        return normalized
    
    def calculate_product_score(self, product: Dict, user_concerns: Dict[str, float]) -> float:
        """
        Calculate a score for a product based on how well it addresses user concerns.
        Uses the ranked order of product concern_tags (most affected -> least) so
        earlier tags contribute more to the score. Ingredient count no longer
        affects scoring.
        """
        product_concerns = product.get("concern_tags", [])
        if not product_concerns or product_concerns == ["general"]:
            return 0.0
        
        score = 0.0
        
        for user_concern, percentage in user_concerns.items():
            if user_concern in product_concerns:
                # Weight contribution by the rank position in concern_tags
                # First tag = weight 1.0, second = 0.5, third = ~0.33, etc.
                rank_index = product_concerns.index(user_concern)
                position_weight = 1.0 / (rank_index + 1)

                # Higher user severity percentage contributes more, scaled by rank weight
                score += percentage * position_weight

                # Severity bonus, also scaled by rank weight
                if percentage >= 70:
                    score += 20 * position_weight  # Bonus for severe concerns
                elif percentage >= 40:
                    score += 10 * position_weight  # Bonus for moderate concerns
        
        return score
    
    def filter_products_by_concerns(self, user_concerns: Dict[str, float], 
                                  min_score_threshold: float = 10.0) -> List[Tuple[Dict, float]]:
        """
        Filter and score products based on user concerns
        Returns list of (product, score) tuples sorted by score
        """
        scored_products = []
        
        for product in self.products:
            score = self.calculate_product_score(product, user_concerns)
            if score >= min_score_threshold:
                scored_products.append((product, score))
        
        # Sort by score (highest first)
        scored_products.sort(key=lambda x: x[1], reverse=True)
        return scored_products

    def create_minimal_prompt(self, user_concerns: Dict[str, float],
                              top_products: List[Tuple[Dict, float]],
                              skin_type: Union[str, None], budget: Union[str, None],
                              num_recommendations: int) -> str:

        concerns_text = []
        for concern, percentage in user_concerns.items():
            if percentage > 0:
                severity = "H" if percentage >= 40 else "M" if percentage >= 20 else "L"
                concerns_text.append(f"{concern}({severity})")

        # Only include top 20 products to save tokens
        trimmed_products = top_products[:10]

        product_list = []
        for product, score in trimmed_products:
            product_info = {
                "name": product["name"][:80],
                "concerns": product.get("concern_tags", [])[:3],  # smaller list
                "brand": product.get("brand", "")[:30],
                "price": product.get("price", ""),
                # drop long ingredient strings, keep only first few
                "ingredients": ", ".join(product.get("ingredients", "").split(",")[:3])
            }
            product_list.append(product_info)

            # Shorter, cleaner prompt
        prompt = f"""Recommend {num_recommendations} skincare products 
    for user concerns: {', '.join(concerns_text)} | Skin: {skin_type or 'any'} | Budget: {budget or 'any'}.

    Products to choose from:
    {json.dumps(product_list, indent=2)}

    {{
      "recommendations": [
        {{
          "name": "product name",
          "reason": "why this product",
          "priority": "H/M/L"
        }}
      ]
    }}
    """
        return prompt

    def parse_llm_response(self, response: str) -> List[Dict]:
        """Parse the LLM response into structured data"""
        try:
            # Try strict JSON first
            data = json.loads(response)
            if "recommendations" in data:
                return data["recommendations"]
            else:
                # Fall back: sometimes the model returns a list directly
                if isinstance(data, list):
                    return data
                # Try to extract JSON substring if the model wrapped text
                start = response.find('{')
                end = response.rfind('}')
                if start != -1 and end != -1 and end > start:
                    try:
                        data_inner = json.loads(response[start:end+1])
                        if "recommendations" in data_inner:
                            return data_inner["recommendations"]
                    except Exception:
                        pass
                return [{"error": "Invalid response format - missing 'recommendations' key"}]
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print(f"Raw response: {response[:200]}...")
            # If the model responded with plain text, return it as a single recommendation reason
            cleaned = response.strip()
            if cleaned:
                return [{"name": "LLM Output", "reason": cleaned[:500], "priority": "Low"}]
            return [{"error": f"JSON parsing error: {str(e)}"}]
        except Exception as e:
            return [{"error": f"Parsing error: {str(e)}"}]


    def get_quick_recommendations(self, user_concerns: Dict[str, float], 
                                 num_recommendations: int = 5) -> List[Dict]:
        """
        Get quick recommendations without LLM (faster, less detailed, no tokens used)
        """
        scored_products = self.filter_products_by_concerns(user_concerns)
        
        recommendations = []
        for i, (product, score) in enumerate(scored_products[:num_recommendations]):
            rec = {
                "product_name": product["name"],
                "brand": product.get("brand", ""),
                "price": product.get("price", ""),
                "score": round(score, 2),
                #"concerns_addressed": product.get("concern_tags", []),
                "reason": f"High match score ({score:.1f}) for your concerns"
            }
            recommendations.append(rec)
        
        return recommendations

    def check_available_concerns(self) -> set:
        """Check what concern tags are available in the products"""
        all_concerns = set()
        for product in self.products:
            concerns = product.get("concern_tags", [])
            all_concerns.update(concerns)
        
        # print(f"\nAvailable concern tags in products:")
        # for concern in sorted(all_concerns):
        #     print(f"  - {concern}")
        
        return all_concerns

    def suggest_concern_corrections(self, user_concerns: Dict[str, float]) -> Dict[str, str]:
        """Suggest corrections for user concerns based on available tags"""
        available_concerns = self.check_available_concerns()
        
        print(f"\nYour concerns: {list(user_concerns.keys())}")
        print(f"Available concerns: {list(available_concerns)[:20]}")
        
        suggestions = {}
        for user_concern in user_concerns.keys():
            # Try to find exact matches
            if user_concern in available_concerns:
                suggestions[user_concern] = user_concern
                print(f"  '{user_concern}' -> Found exact match")
            else:
                # Try to find similar concerns
                similar = []
                for available in available_concerns:
                    if (user_concern.lower() in available.lower() or 
                        available.lower() in user_concern.lower()):
                        similar.append(available)
                
                if similar:
                    suggestions[user_concern] = similar[0]
                    print(f"  '{user_concern}' -> '{similar[0]}' (suggested)")
                else:
                    print(f"  '{user_concern}' -> No match found")
        
        return suggestions

# Usage example
if __name__ == "__main__":
    # Example usage
    try:
        load_dotenv()
        api_key = os.getenv("API_KEY")
        print(api_key)
        engine = SkincareRecommendationEngine(api_key=api_key)
        if not engine.products:
            print("Cannot proceed without product data. Please ensure you have run the concern tagging script first.")
            exit(1)
        
        # Check what concerns are available
        engine.check_available_concerns()
        
        # User concerns with percentages
        user_concerns = {
            "anti-aging": 60,        # Severe acne concern
            "dark circles": 40,  # Moderate hyperpigmentation
            "pores": 40,    # Mild dryness
            "acne" : 40,
            "dryness": 50
        }

        suggestions = engine.suggest_concern_corrections(user_concerns)
        
        # Get detailed LLM recommendations (uses tokens)
        print("\nGetting LLM recommendations...")
        recommendations = engine.get_recommendations(
            user_concerns=user_concerns,
            skin_type="dry",
            budget="$50",
            num_recommendations=5
        )

        print("\nGetting quick recommendations...")
        quick_recs = engine.get_quick_recommendations(user_concerns, num_recommendations=5)
        
        print("\nDetailed Recommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec.get('name', 'N/A')}")
            print(f"   Reason: {rec.get('reason', 'N/A')}")
            print(f"   Priority: {rec.get('priority', 'N/A')}")
            print()
            
        print("\nQuick Recommendations:")
        for i, rec in enumerate(quick_recs, 1):
            print(f"{i}. {rec.get('product_name', 'N/A')}")
            print(f"   Score: {rec.get('score', 'N/A')}")
            print(f"   Brand: {rec.get('brand', 'N/A')}")
            print(f"   Price: {rec.get('price', 'N/A')}")
            print()
            
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please check your API key and data files.")
