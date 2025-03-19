# Mistral_connect

from mistralai import Mistral
import os

# Load environment variables
api_key = os.environ["MISTRAL_API_KEY"]


# function Mistral
def mistral_con():
    return Mistral(api_key=api_key)
