# Generated by Django 3.1.2 on 2020-11-06 12:38

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Group',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('parent_id', models.IntegerField(blank=True, null=True)),
                ('group_name', models.CharField(max_length=4000, unique=True)),
                ('strg_period', models.IntegerField(blank=True, null=True)),
                ('strg_period_type', models.CharField(blank=True, max_length=1, null=True)),
            ],
        ),
    ]
