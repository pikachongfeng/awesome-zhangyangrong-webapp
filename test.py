import orm
from models import User, Blog, Comment

def test():
    await orm.create_pool(user='www-data', password='www-data', database='awesome')

    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')

    await u.save()

for x in test():
    pass