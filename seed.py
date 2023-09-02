from faker import Faker
from random import randint, choice
from sqlalchemy import select
from datetime import date, datetime, timedelta

from source.db_connect import session
from source.models import Teacher, Student, Discipline, Grade, Group

fake = Faker()

TEACHERS = 5
STUDENTS = 30
GRADES = 20
groups = ['Group A', 'Group B', 'Group C'] 
disciplines = [
    'Math', 
    'Physics', 
    'Chemistry', 
    'Biology', 
    'History']


def seed_groups():
    for group in groups:
            session.add(Group(name=group))
    session.commit()


def seed_teachers():
    for _ in range(TEACHERS):
        teacher = Teacher(fullname=fake.name())
        session.add(teacher)
    session.commit()

def seed_students():
    group_ids = session.scalars(select(Group.id)).all()
    for _ in range(STUDENTS):
        student = Student(fullname=fake.name(), group_id=choice(group_ids))
        session.add(student)
        session.commit()


def seed_disciplines():
    teacher_ids = session.scalars(select(Teacher.id)).all()  
    for discipline in disciplines:
        session.add(Discipline(name=discipline, teacher_id=choice(teacher_ids)))
    session.commit()


def date_range(start: date, end: date) -> list:
    result = []
    current_date = start
    while current_date <= end:
        if current_date.isoweekday() < 6:
            result.append(current_date)
        current_date += timedelta(1)
    return result

def seed_grades():
    start_date = datetime.strptime("2022-09-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-05-30", "%Y-%m-%d")
    d_range = date_range(start=start_date, end=end_date)
    discipline_ids = session.scalars(select(Discipline.id)).all()
    student_ids = session.scalars(select(Student.id)).all()

    for d in d_range:  
            random_id_discipline = choice(discipline_ids)
            random_ids_student = [choice(student_ids) for _ in range(5)]

            for student_id in random_ids_student:
                grade = Grade(
                    grade=randint(1, 12),
                    date_of=d,
                    student_id=student_id,
                    discipline_id=random_id_discipline,
                )
                session.add(grade)
    session.commit()

if __name__ == "__main__":
    seed_groups()
    seed_students()
    seed_teachers()
    seed_disciplines()
    seed_grades()

