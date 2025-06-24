#!/usr/bin/env python3
import requests
import csv
import json
import os
import sys
from typing import List, Dict, Any
from datetime import datetime

def fetch_users(token: str) -> List[Dict[str, Any]]:
    """Fetch users from the GraphQL API."""
    url = f"{os.environ.get('CONSOLE_URL')}/gql"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ.get('BEARER_TOKEN')}"
    }
    
    query = """
    query {
        users(first: 100) {
            edges {
                node {
                    id
                    name
                    email
                    roles {
                        admin
                    }
                    pluralId
                    deletedAt
                    profile
                    insertedAt
                    updatedAt
                    groups {
                        name
                    }
                }
            }
            pageInfo {
                hasNextPage
                endCursor
            }
        }
    }
    """
    
    try:
        response = requests.post(url, headers=headers, json={"query": query})
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
            
        return [edge["node"] for edge in data["data"]["users"]["edges"]]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch users: {str(e)}")

def process_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """Process a user object to extract relevant fields."""
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "admin": user["roles"]["admin"] if user["roles"] else False,
        "plural_id": user["pluralId"],
        "deleted_at": user["deletedAt"],
        "profile": user["profile"],
        "inserted_at": user["insertedAt"],
        "updated_at": user["updatedAt"],
        "groups": ";".join([g["name"] for g in user["groups"]]) if user["groups"] else ""
    }

def export_to_csv(users: List[Dict[str, Any]], filename: str = "users.csv"):
    """Export users to a CSV file."""
    fieldnames = [
        "id", "name", "email", "admin", "plural_id",
        "deleted_at", "profile", "inserted_at", "updated_at", "groups"
    ]
    
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for user in users:
                writer.writerow(process_user(user))
        print(f"Successfully exported {len(users)} users to {filename}")
    except IOError as e:
        raise Exception(f"Failed to write CSV file: {str(e)}")

def main():
    if not os.environ.get('BEARER_TOKEN'):
        print("Error: BEARER_TOKEN environment variable is not set")
        sys.exit(1)
    if not os.environ.get('CONSOLE_URL'):
        print("Error: CONSOLE_URL environment variable is not set")
        sys.exit(1)

    try:
        users = fetch_users(os.environ.get('BEARER_TOKEN'))
        export_to_csv(users)
        print(f"Successfully exported {len(users)} users to users.csv")
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()