-- These queries use the data from the notes, ratings, and enrollment_status tables to work out the IDs of participants, because this information is not directly provided by Twitter

INSERT INTO participants ("participantId", "created_at", "updated_at") (SELECT DISTINCT "noteAuthorParticipantId", LOCALTIMESTAMP, LOCALTIMESTAMP FROM notes) ON CONFLICT DO NOTHING;

INSERT INTO participants ("participantId", "created_at", "updated_at") (SELECT DISTINCT "raterParticipantId", LOCALTIMESTAMP, LOCALTIMESTAMP FROM ratings) ON CONFLICT DO NOTHING;

INSERT INTO participants ("participantId", "created_at", "updated_at") (SELECT DISTINCT "participantId", LOCALTIMESTAMP, LOCALTIMESTAMP FROM enrollment_status) ON CONFLICT DO NOTHING;