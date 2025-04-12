def store_in_database(db_connection, document_data, file_name):
    cursor = db_connection.cursor()
    try:
        # Insert into trace.documents
        cursor.execute(
            "INSERT INTO trace.documents (document_name, document_type, full_text) VALUES (%s, %s, %s) RETURNING id",
            (file_name, 'course_evaluation', document_data['full_text'])
        )
        document_id = cursor.fetchone()[0]

        # Insert into trace.course_info if available
        course_info = document_data.get('course_info', {})
        if course_info:
            catalog_section = course_info.get('catalog_section', '')
            course_number = f"{course_info.get('subject', '')} {catalog_section.split()[0]}" if catalog_section else ''
            section = catalog_section.split()[1] if len(catalog_section.split()) > 1 else ''

            cursor.execute("""
                INSERT INTO trace.course_info 
                (document_id, course_name, course_number, section, semester, year, instructor_name, enrollment_count, response_count, declines_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    course_info.get('course_name', ''),
                    course_number,
                    section,
                    course_info.get('semester', ''),
                    course_info.get('year', 0),
                    course_info.get('instructor', ''),
                    course_info.get('enrollment', 0),
                    course_info.get('responses', 0),
                    course_info.get('declines', 0)
                )
            )

        # Insert into trace.course_ratings
        for rating in document_data.get('ratings', []):
            cursor.execute("""
                INSERT INTO trace.course_ratings
                (document_id, category, question, response_count, course_mean)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    rating.get('category', ''),
                    rating.get('question', ''),
                    rating.get('response_count', None),
                    rating.get('course_mean', None)
                )
            )

        # Insert into trace.student_comments
        for comment in document_data.get('comments', []):
            cursor.execute("""
                INSERT INTO trace.student_comments
                (document_id, question_category, question, comment_number, comment_text)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    document_id,
                    'Student Feedback',
                    comment.get('question', ''),
                    comment.get('comment_number', 0),
                    comment.get('text', '')
                )
            )

        db_connection.commit()
        print(f"✅ Successfully stored {file_name} in database with ID {document_id}")
        return document_id

    except Exception as e:
        db_connection.rollback()
        print(f"❌ Error storing document in database: {e}")
        return None
