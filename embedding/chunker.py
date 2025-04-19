import psycopg2
from config.settings import DB_CONFIG

def infer_chunk_type_from_question(question):
    q = question.lower()
    if "strength" in q and "inclusive" not in q:
        return "strengths"
    elif "make this course better" in q and "instructor" in q:
        return "improvements"
    elif "inclusive learning" in q:
        return "inclusive_learning"
    elif "online course environment" in q:
        return "online_experience"
    elif "for myself" in q:
        return "self_reflection"
    else:
        return "student_comment"

def chunk_document_data(document):
    chunks = []

    # Comment chunks grouped by question type
    grouped_comments = {}
    for comment in document.get('comments', []):
        chunk_type = infer_chunk_type_from_question(comment['question'])
        if chunk_type not in grouped_comments:
            grouped_comments[chunk_type] = []
        grouped_comments[chunk_type].append(comment)

    for chunk_type, comments in grouped_comments.items():
        combined_text = "\n\n".join([f"{c['comment_number']}. {c['text']}" for c in comments])
        question_header = comments[0]['question'] if comments else ""
        chunks.append({
            'id': f"{document['document_id']}_{chunk_type}",
            'text': f"Q: {question_header}\n\n{combined_text}",
            'chunk_type': chunk_type,
            'professor': document['professor']
        })

    return chunks

def generate_chunks_from_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT d.id, d.document_name, d.full_text, ci.instructor_name
        FROM documents d
        JOIN course_info ci ON d.id = ci.document_id
    """)
    documents = cursor.fetchall()

    cursor.execute("""
        SELECT document_id, question, comment_number, comment_text
        FROM student_comments
        ORDER BY document_id, comment_number
    """)
    comments = cursor.fetchall()
    conn.close()

    comments_by_doc = {}
    for doc_id, question, number, text in comments:
        if doc_id not in comments_by_doc:
            comments_by_doc[doc_id] = []
        comments_by_doc[doc_id].append({
            'question': question,
            'comment_number': number,
            'text': text
        })

    all_chunks = []
    for doc_id, name, full_text, instructor_name in documents:
        document = {
            'document_id': doc_id,
            'document_name': name,
            'full_text': full_text,
            'comments': comments_by_doc.get(doc_id, []),
            'professor': instructor_name
        }
        all_chunks.extend(chunk_document_data(document))

    return all_chunks
