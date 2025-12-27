"""
Feedback Router - Kupon, tahmin ve bültenler için beğeni/beğenmeme sistemi
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from ..services.db import query_to_df

router = APIRouter()

# Başlangıç offset - tüm içerikler +15 beğeni ile başlar
INITIAL_LIKES_OFFSET = 15


class FeedbackCreate(BaseModel):
    """Feedback oluşturma şeması"""
    user_id: str  # UUID string
    content_type: str  # 'prediction', 'coupon', 'newsletter'
    content_id: str
    feedback_type: str  # 'like' veya 'dislike'


class FeedbackResponse(BaseModel):
    """Feedback yanıt şeması"""
    id: int
    user_id: str
    content_type: str
    content_id: str
    feedback_type: str
    created_at: datetime


class FeedbackCountResponse(BaseModel):
    """Feedback sayı yanıt şeması"""
    content_type: str
    content_id: str
    likes: int
    dislikes: int
    user_feedback: Optional[str] = None  # Kullanıcının verdiği feedback


def _row_to_response(row) -> FeedbackResponse:
    """DataFrame satırını response modeline çevir"""
    return FeedbackResponse(
        id=int(row['id']),
        user_id=str(row['user_id']),
        content_type=row['content_type'],
        content_id=row['content_id'],
        feedback_type=row['feedback_type'],
        created_at=row['created_at']
    )


@router.post("/feedback", response_model=FeedbackResponse)
async def create_or_update_feedback(feedback: FeedbackCreate):
    """
    Feedback oluştur veya güncelle.
    Aynı kullanıcı aynı içeriğe tekrar tıklarsa feedback güncellenir.
    Aynı feedback_type'a tekrar tıklarsa feedback silinir (toggle).
    """
    import traceback
    try:
        # Mevcut feedback'i kontrol et
        check_sql = """
            SELECT id, feedback_type FROM greydb.feedbacks
            WHERE user_id = %s AND content_type = %s AND content_id = %s
        """
        existing = query_to_df(check_sql, (feedback.user_id, feedback.content_type, feedback.content_id))
        
        if not existing.empty:
            existing_type = existing.iloc[0]['feedback_type']
            existing_id = int(existing.iloc[0]['id'])
            
            if existing_type == feedback.feedback_type:
                # Aynı butona tekrar tıklandı - feedback'i sil (toggle off)
                delete_sql = "DELETE FROM greydb.feedbacks WHERE id = %s"
                query_to_df(delete_sql, (existing_id,), commit=True)
                raise HTTPException(status_code=204, detail="Feedback removed")
            else:
                # Farklı butona tıklandı - güncelle
                update_sql = """
                    UPDATE greydb.feedbacks 
                    SET feedback_type = %s
                    WHERE id = %s
                    RETURNING *
                """
                result = query_to_df(update_sql, (feedback.feedback_type, existing_id), commit=True)
                return _row_to_response(result.iloc[0])
        
        # Yeni feedback oluştur
        insert_sql = """
            INSERT INTO greydb.feedbacks (user_id, content_type, content_id, feedback_type)
            VALUES (%s, %s, %s, %s)
            RETURNING *
        """
        result = query_to_df(
            insert_sql,
            (feedback.user_id, feedback.content_type, feedback.content_id, feedback.feedback_type),
            commit=True
        )
        return _row_to_response(result.iloc[0])
    except HTTPException:
        raise
    except Exception as e:
        print(f"Feedback error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Feedback error: {str(e)}")


@router.get("/feedback/counts/{content_type}/{content_id}", response_model=FeedbackCountResponse)
async def get_feedback_counts(content_type: str, content_id: str, user_id: Optional[str] = None):
    """
    Belirli bir içeriğin feedback sayılarını getir.
    user_id verilirse kullanıcının verdiği feedback de döner.
    """
    # Feedback sayılarını al
    count_sql = """
        SELECT 
            COUNT(*) FILTER (WHERE feedback_type = 'like') as likes,
            COUNT(*) FILTER (WHERE feedback_type = 'dislike') as dislikes
        FROM greydb.feedbacks
        WHERE content_type = %s AND content_id = %s
    """
    counts = query_to_df(count_sql, (content_type, content_id))
    
    likes = int(counts.iloc[0]['likes']) + INITIAL_LIKES_OFFSET if not counts.empty else INITIAL_LIKES_OFFSET
    dislikes = int(counts.iloc[0]['dislikes']) if not counts.empty else 0
    
    # Kullanıcının feedback'ini al
    user_feedback = None
    if user_id:
        user_sql = """
            SELECT feedback_type FROM greydb.feedbacks
            WHERE user_id = %s AND content_type = %s AND content_id = %s
        """
        user_result = query_to_df(user_sql, (user_id, content_type, content_id))
        if not user_result.empty:
            user_feedback = user_result.iloc[0]['feedback_type']
    
    return FeedbackCountResponse(
        content_type=content_type,
        content_id=content_id,
        likes=likes,
        dislikes=dislikes,
        user_feedback=user_feedback
    )


@router.get("/feedback/bulk-counts/{content_type}", response_model=List[FeedbackCountResponse])
async def get_bulk_feedback_counts(content_type: str, content_ids: str, user_id: Optional[str] = None):
    """
    Birden fazla içeriğin feedback sayılarını tek seferde getir.
    content_ids: virgülle ayrılmış ID listesi (örn: "1,2,3")
    """
    ids_list = [id.strip() for id in content_ids.split(',')]
    
    # Tüm feedback'leri al
    placeholders = ','.join(['%s'] * len(ids_list))
    count_sql = f"""
        SELECT 
            content_id,
            COUNT(*) FILTER (WHERE feedback_type = 'like') as likes,
            COUNT(*) FILTER (WHERE feedback_type = 'dislike') as dislikes
        FROM greydb.feedbacks
        WHERE content_type = %s AND content_id IN ({placeholders})
        GROUP BY content_id
    """
    counts = query_to_df(count_sql, (content_type, *ids_list))
    
    # Kullanıcı feedback'lerini al
    user_feedbacks = {}
    if user_id:
        user_sql = f"""
            SELECT content_id, feedback_type FROM greydb.feedbacks
            WHERE user_id = %s AND content_type = %s AND content_id IN ({placeholders})
        """
        user_result = query_to_df(user_sql, (user_id, content_type, *ids_list))
        for _, row in user_result.iterrows():
            user_feedbacks[row['content_id']] = row['feedback_type']
    
    # Sonuçları oluştur
    results = []
    counts_dict = {}
    for _, row in counts.iterrows():
        counts_dict[row['content_id']] = {
            'likes': int(row['likes']),
            'dislikes': int(row['dislikes'])
        }
    
    for content_id in ids_list:
        data = counts_dict.get(content_id, {'likes': 0, 'dislikes': 0})
        results.append(FeedbackCountResponse(
            content_type=content_type,
            content_id=content_id,
            likes=data['likes'] + INITIAL_LIKES_OFFSET,
            dislikes=data['dislikes'],
            user_feedback=user_feedbacks.get(content_id)
        ))
    
    return results


@router.delete("/feedback/{content_type}/{content_id}")
async def delete_user_feedback(content_type: str, content_id: str, user_id: str):
    """Kullanıcının feedback'ini sil"""
    delete_sql = """
        DELETE FROM greydb.feedbacks
        WHERE user_id = %s AND content_type = %s AND content_id = %s
    """
    query_to_df(delete_sql, (user_id, content_type, content_id), commit=True)
    return {"message": "Feedback deleted"}

