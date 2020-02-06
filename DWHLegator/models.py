from django.conf import settings
from django.db import models
from django.utils import timezone

# Create your models here.

class Employee(models.Model):
    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    key = models.CharField(max_length=256)
    name = models.CharField(max_length=1000)
    age = models.BigIntegerField()
    position = models.CharField(max_length=256)
    salary = models.DecimalField(max_digits=8,decimal_places=2)
    bonus = models.DecimalField(max_digits=3,decimal_places=2)

    def publish(self):
        self.published_date = timezone.now()
        self.safe()

    def __str__(self):
        return """key=%s: (name=%s, age=%s, position=%s salary=%s, bonus=%s"""%(self.key,self.name,self.age,self.position,self.salary,self.bonus)
