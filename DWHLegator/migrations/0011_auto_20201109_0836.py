# Generated by Django 3.1.2 on 2020-11-09 03:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0010_sqllist'),
    ]

    operations = [
        migrations.AlterField(
            model_name='sqllist',
            name='model_name',
            field=models.CharField(max_length=256, unique=True),
        ),
    ]
