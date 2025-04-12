import psycopg2
from config.settings import DB_CONFIG

def chunk_document_data(document):
    chunks = []

    # Summary chunk
    chunks.append({
        'id': f"{document['document_id']}_summary",
        'text': document['full_text'],
        'chunk_type': 'summary',
        'professor': document['professor']  # ✅ include professor
    })

    # Comment chunks
    for comment in document.get('comments', []):
        chunks.append({
            'id': f"{document['document_id']}_comment_{comment['comment_number']}",
            'text': f"Q: {comment['question']}\\nA: {comment['text']}",
            'chunk_type': 'student_comment',
            'professor': document['professor']  # ✅ include professor
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
            'professor': instructor_name  # ✅ key line
        }
        all_chunks.extend(chunk_document_data(document))

    return all_chunks

