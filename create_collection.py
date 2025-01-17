import weaviate
from weaviate.classes.config import Configure

client = weaviate.connect_to_local()
client.collections.delete('News_db')
questions = client.collections.create(
    name="News_db",
    vectorizer_config=Configure.Vectorizer.text2vec_ollama(
        api_endpoint="http://host.docker.internal:11434",
        model="minilm-gpu"
    ),
    generative_config=Configure.Generative.ollama(
        api_endpoint="http://host.docker.internal:11434",
        model="news-llama3"
    )
)
print('Database Created')
client.close()