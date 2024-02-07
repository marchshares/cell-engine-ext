import os

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def compare_files(file1_path: str, file2_path: str):
    # Read the contents of the files
    with open(file1_path, 'r', encoding='utf-8') as file1:
        text1 = file1.read()
    with open(file2_path, 'r', encoding='utf-8') as file2:
        text2 = file2.read()

    # Create TF-IDF vectors for the documents
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform([text1, text2])

    # Compute cosine similarity between the vectors
    similarity = cosine_similarity(tfidf_matrix[0], tfidf_matrix[1])[0][0]

    # Convert similarity to percentage
    similarity_percentage = similarity * 100

    print(f"Similarity: {os.path.basename(file1_path)} - {os.path.basename(file2_path)}: {similarity_percentage}%")
    return similarity_percentage
