import openai
from typing import List, Dict, Union
import os
import json
from dotenv import load_dotenv

class SkincareLLMOnlyEngine:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        print(f"Initialized Skincare LLM Only Engine with model: {self.model}")
    
    def get_recommendations(self, user_concerns: Dict[str, float], 
                           skin_type: Union[str, None] = None, 
                           budget: Union[str, None] = None, 
                           num_recommendations: int = 5) -> List[Dict]:
        """
        Get personalized skincare recommendations using only LLM knowledge
        
        Args:
            user_concerns: Dict where keys are concerns and values are percentages (0-100)
            skin_type: User's skin type (dry, oily, combination, sensitive, normal)
            budget: Budget constraint (e.g., "$20", "$50", "$100+")
            num_recommendations: Number of products to recommend
        """
        
        # Create a comprehensive prompt for the LLM
        prompt = self.create_comprehensive_prompt(user_concerns, skin_type, budget, num_recommendations)
        
        print(f"Sending prompt to LLM (length: {len(prompt)} chars)...")
        
        try:
            print(f"Calling OpenAI API with model: {self.model}")
            
            response = self.client.responses.create(
                model=self.model,
                instructions="You are a certified dermatologist and skincare expert with extensive knowledge of skincare products, ingredients, and treatments. Provide evidence-based recommendations.",
                input=prompt,
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
                                            "product_name": {"type": "string"},
                                            "brand": {"type": "string"},
                                            "category": {"type": "string"},
                                            "key_ingredients": {"type": "array", "items": {"type": "string"}},
                                            "reason": {"type": "string"},
                                            "priority": {"type": "string"},
                                            "estimated_price": {"type": "string"},
                                            "usage_frequency": {"type": "string"}
                                        },
                                        "required": ["product_name", "brand", "category", "key_ingredients", "reason",
                                                     "priority", "estimated_price", "usage_frequency"],
                                        "additionalProperties": False
                                    }
                                },
                                "skincare_routine": {
                                    "type": "object",
                                    "properties": {
                                        "morning": {"type": "array", "items": {"type": "string"}},
                                        "evening": {"type": "array", "items": {"type": "string"}},
                                        "additional_tips": {"type": "array", "items": {"type": "string"}}
                                    },
                                    "required": ["morning", "evening", "additional_tips"],
                                    "additionalProperties": False
                                }
                            },
                            "required": ["recommendations", "skincare_routine"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
            
            print("Response received from LLM")
            print(response.output_text)
            
            if response:
                return self.parse_llm_response(response.output_text)
            else:
                return [{"error": "No response content received"}]

        except Exception as e:
            print(f"API call error: {e}")
            return [{"error": f"API call failed: {str(e)}"}]
    
    def create_comprehensive_prompt(self, user_concerns: Dict[str, float],
                                  skin_type: Union[str, None], 
                                  budget: Union[str, None],
                                  num_recommendations: int) -> str:
        """Create a comprehensive prompt for the LLM"""
        
        # Format concerns with severity levels
        concerns_text = []
        for concern, percentage in user_concerns.items():
            if percentage > 0:
                severity = "High" if percentage >= 60 else "Medium" if percentage >= 30 else "Low"
                concerns_text.append(f"{concern} ({severity} - {percentage}%)")
        
        # Create detailed prompt
        prompt = f"""As a certified dermatologist and skincare expert, provide personalized skincare recommendations for a user with the following profile:

**User Profile:**
- Primary Concerns: {', '.join(concerns_text)}
- Skin Type: {skin_type or 'Not specified'}
- Budget: {budget or 'No specific budget'}
- Number of recommendations needed: {num_recommendations}

**Requirements:**
1. Recommend {num_recommendations} specific skincare products that address the user's concerns
2. Include both drugstore and high-end options within the budget
3. Provide a complete skincare routine (morning and evening)
4. Consider skin type compatibility
5. Focus on evidence-based ingredients
6. Include usage frequency and application tips

**Response Format:**
Provide recommendations in the exact JSON format specified, including:
- Product name and brand
- Product category (cleanser, moisturizer, serum, etc.)
- Key active ingredients
- Detailed reasoning for each recommendation
- Priority level (High/Medium/Low)
- Estimated price range
- Usage frequency
- Complete morning and evening routine
- Additional skincare tips

Consider the following when making recommendations:
- Ingredient compatibility
- Skin type suitability
- Budget constraints
- Scientific evidence for effectiveness
- Potential side effects or contraindications

Please provide specific, actionable recommendations that the user can implement immediately."""

        return prompt

    def parse_llm_response(self, response: str) -> List[Dict]:
        """Parse the LLM response into structured data"""
        try:
            data = json.loads(response)
            
            # Extract recommendations
            recommendations = data.get("recommendations", [])
            
            # Extract skincare routine
            skincare_routine = data.get("skincare_routine", {})
            
            # Combine both into a comprehensive result
            result = {
                "recommendations": recommendations,
                "skincare_routine": skincare_routine
            }
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print(f"Raw response: {response[:200]}...")
            return [{"error": f"JSON parsing error: {str(e)}"}]
        except Exception as e:
            return [{"error": f"Parsing error: {str(e)}"}]

    def get_ingredient_analysis(self, ingredients: List[str]) -> Dict:
        """
        Get analysis of skincare ingredients using LLM
        """
        prompt = f"""Analyze these skincare ingredients and provide detailed information:

Ingredients: {', '.join(ingredients)}

For each ingredient, provide:
1. What it does
2. Benefits for skin
3. Potential side effects

Format as JSON with ingredient analysis."""

        try:
            response = self.client.responses.create(
                model=self.model,
                instructions="You are a cosmetic chemist and skincare ingredient expert.",
                input=prompt,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "ingredient_analysis",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "ingredients": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "function": {"type": "string"},
                                            "benefits": {"type": "array", "items": {"type": "string"}},
                                            "side_effects": {"type": "array", "items": {"type": "string"}},
                                            "skin_types": {"type": "array", "items": {"type": "string"}},
                                            "evidence_level": {"type": "string"}
                                        },
                                        "required": ["name", "function", "benefits", "side_effects", "skin_types", "evidence_level"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["ingredients"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
            
            return self.parse_llm_response(response.output_text)
            
        except Exception as e:
            return {"error": f"Ingredient analysis failed: {str(e)}"}

# Usage example
if __name__ == "__main__":
    try:
        load_dotenv()
        api_key = os.getenv("API_KEY")
        
        if not api_key:
            print("Error: API_KEY not found in environment variables")
            exit(1)
        
        engine = SkincareLLMOnlyEngine(api_key=api_key)
        
        # Example user concerns
        user_concerns = {
            "acne": 70,
            "hyperpigmentation": 40,
            "aging": 30,
            "dryness": 20
        }
        
        print("Getting LLM-only skincare recommendations...")
        result = engine.get_recommendations(
            user_concerns=user_concerns,
            skin_type="combination",
            budget="$100",
            num_recommendations=5
        )
        
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print("\n=== SKINCARE RECOMMENDATIONS ===")
            recommendations = result.get("recommendations", [])
            for i, rec in enumerate(recommendations, 1):
                print(f"\n{i}. {rec.get('product_name', 'N/A')}")
                print(f"   Brand: {rec.get('brand', 'N/A')}")
                print(f"   Category: {rec.get('category', 'N/A')}")
                print(f"   Key Ingredients: {', '.join(rec.get('key_ingredients', []))}")
                print(f"   Reason: {rec.get('reason', 'N/A')}")
                print(f"   Priority: {rec.get('priority', 'N/A')}")
                print(f"   Price: {rec.get('estimated_price', 'N/A')}")
                print(f"   Usage: {rec.get('usage_frequency', 'N/A')}")
            
            print("\n=== SKINCARE ROUTINE ===")
            routine = result.get("skincare_routine", {})
            
            print("\nMorning Routine:")
            for step in routine.get("morning", []):
                print(f"  • {step}")
            
            print("\nEvening Routine:")
            for step in routine.get("evening", []):
                print(f"  • {step}")
            
            print("\nAdditional Tips:")
            for tip in routine.get("additional_tips", []):
                print(f"  • {tip}")
        
        # Example ingredient analysis
        print("\n=== INGREDIENT ANALYSIS EXAMPLE ===")
        ingredients = ["retinol", "vitamin C", "hyaluronic acid", "niacinamide"]
        analysis = engine.get_ingredient_analysis(ingredients)
        
        if "error" not in analysis:
            for ingredient in analysis.get("ingredients", []):
                print(f"\n{ingredient['name'].upper()}:")
                print(f"  Function: {ingredient['function']}")
                print(f"  Benefits: {', '.join(ingredient['benefits'])}")
                print(f"  Side Effects: {', '.join(ingredient['side_effects'])}")
                print(f"  Best for: {', '.join(ingredient['skin_types'])}")
                print(f"  Evidence Level: {ingredient['evidence_level']}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please check your API key and ensure it's set in your .env file")
