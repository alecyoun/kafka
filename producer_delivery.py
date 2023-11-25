import random
import json
import time
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

def generate_fake_data(event_type):
    common_data = {
        'user_id': fake.random_int(min=1, max=100),
    }

    if event_type == '사용자 등록':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'username': fake.user_name(),
            'email': fake.email(),
        }
    elif event_type == '사용자 로그인':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'menu_id': fake.random_int(min=1, max=100),
            'quantity': fake.random_int(min=1, max=5),
        }
    elif event_type == '주문 생성':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'menu_id': fake.random_int(min=1, max=100),
            'quantity': fake.random_int(min=1, max=5),
        }
    elif event_type == '결제 처리':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'order_id': fake.random_int(min=1, max=10000),
            'total_price': fake.random_int(min=5000, max=30000, step=1000),
        }
    elif event_type == '배송 상태 조회':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'order_id': fake.random_int(min=1, max=10000),
            'delivery_status': fake.random_element(elements=('배송 준비 중', '배송 중', '배송 완료')),
        }
    elif event_type == '배송 위치 조회':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'location_id': fake.random_int(min=1, max=100),
            'latitude': fake.latitude(),
            'longitude': fake.longitude(),
        }
    elif event_type == '알림 조회':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'notification_id': fake.random_int(min=1, max=100),
            'message': fake.text(),
        }
    elif event_type == '리뷰 조회':
        return {
            'event_type': event_type,
            'user_id': common_data['user_id'],
            'order_id': fake.random_int(min=1, max=10000),
            'rating': fake.random_int(min=1, max=5),
            'comment': fake.text(),
        }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def send_fake_event(producer, event_type):
    fake_data = generate_fake_data(event_type)
    print(f"{event_type} 이벤트 발생")
    print(fake_data)
    producer.send(f'{event_type.lower().replace(" ", "_")}_event', fake_data)
    print("=============================\n")

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=json_serializer
    )

    # 10개의 더미 데이터 생성
    for _ in range(10000):
        send_fake_event(producer, '사용자 등록')
        send_fake_event(producer, '사용자 로그인')
        send_fake_event(producer, '주문 생성')
        send_fake_event(producer, '결제 처리')
        send_fake_event(producer, '배송 상태 조회')
        send_fake_event(producer, '배송 위치 조회')
        send_fake_event(producer, '알림 조회')
        send_fake_event(producer, '리뷰 조회')
        time.sleep(1)
