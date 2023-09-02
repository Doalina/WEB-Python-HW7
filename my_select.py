from sqlalchemy import func, desc, and_, select

from source.db_connect import session
from source.models import Teacher, Student, Discipline, Grade, Group


# Знайти 5 студентів із найбільшим середнім балом з усіх предметів
def select_1():  
    result = (
        session.query(
            Student.fullname, func.round(func.avg(Grade.grade), 1).label("avg_grade")
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("avg_grade"))
        .limit(5)
        .all()
    )
    return result

# Знайти студента із найвищим середнім балом з певного предмета
def select_2(discipline_id: int):
    result = (
        session.query(
            Discipline.name,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Discipline.id == discipline_id)
        .group_by(Student.id, Discipline.name)
        .order_by(desc("avg_grade"))
        .limit(1)
        .all()
    )
    return result


# Знайти середній бал у групах з певного предмета
def select_3(discipline_id: int):
    result = (
        session.query(
            Discipline.name,
            Group.name,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Group)
        .join(Discipline)
        .filter(Discipline.id == discipline_id)
        .group_by(Group.name, Discipline.name)
        .order_by(desc('avg_grade')) 
        .all()
    )
    return result

# Знайти середній бал на потоці (по всій таблиці оцінок)
def select_4():
    result = (
        session.query(func.round(func.avg(Grade.grade), 2)).select_from(Grade).all()
    )
    return result


# Знайти які курси читає певний викладач
def select_5(teacher_id: int):
    result = (
        session.query(Teacher.fullname, Discipline.name) 
        .select_from(Discipline) 
        .join(Teacher) 
        .filter(Teacher.id == teacher_id) 
        .group_by(Teacher.fullname, Discipline.name) 
        .all()
    )
    return result


# Знайти список студентів у певній групі
def select_6(group_id: int):
    result = (
        session.query(Group.name, Student.fullname)
        .select_from(Student)
        .join(Group)
        .filter(Group.id == group_id)
        .group_by(Group.id, Student.fullname)
        .all()
    )
    return result

# Знайти оцінки студентів у окремій групі з певного предмета
def select_7(group_id: int, discipline_id: int):
    result = (
        session.query(Student.fullname, Discipline.name, Grade.grade) 
        .select_from(Grade) 
        .join(Student) 
        .join(Discipline) 
        .filter(and_(Student.group_id == group_id, Discipline.id == discipline_id)) 
        .order_by(Discipline.name) 
        .all()
    )
    return result

# Знайти середній бал, який ставить певний викладач зі своїх предметів
def select_8(teacher_id: int):
    result = (
        session.query(
            Teacher.fullname,
            Discipline.name,
            func.round(func.avg(Grade.grade), 2).label("avg_grade"),
        )
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.fullname, Discipline.name) \
        .all()
    )
    return result

# Знайти список курсів, які відвідує студент
def select_9(student_id: int):
    result = (
        session.query(Student.fullname, Discipline.name)
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Student.id == student_id)
        .group_by(Student.fullname, Discipline.name)
        .all()
    )
    return result

# Список курсів, які певному студенту читає певний викладач
def select_10(student_id: int, teacher_id: int):
    subquery = (select(Discipline.id).where(Discipline.teacher_id == teacher_id).scalar_subquery())
    result = (
        session.query(Discipline.name) 
        .select_from(Grade) 
        .join(Discipline) 
        .filter(and_(Grade.discipline_id.in_(subquery), Grade.student_id == student_id)) 
        .group_by(Discipline.name) 
        .all()
    )
    return result


if __name__ == '__main__':
    print(select_1())
    print(select_2(2))
    print(select_3(1))
    print(select_4())
    print(select_5(1))
    print(select_6(1))
    print(select_7(1,1))
    print(select_8(1))
    print(select_9(1))
    print(select_10(1, 1))
