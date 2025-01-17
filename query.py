import weaviate
import json

client = weaviate.connect_to_local()

questions = client.collections.get("News_db")

# response = questions.query.near_text(
#     query="biden",
#     limit=10
# )

response = questions.generate.near_text(
    query="Biden",
    grouped_task="Translate The News To French",
    limit=2
)
print(response.generated)
# for obj in response.objects:
#     print(json.dumps(obj.properties, indent=2))

client.close()  # Free up resources