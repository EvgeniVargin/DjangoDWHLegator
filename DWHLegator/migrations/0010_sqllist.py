# Generated by Django 3.1.2 on 2020-11-09 03:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0009_auto_20201108_2113'),
    ]

    operations = [
        migrations.CreateModel(
            name='SQLlist',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('model_name', models.CharField(max_length=256)),
                ('checkout_sql', models.TextField(blank=True, null=True)),
                ('checkout_sql_all', models.TextField(blank=True, null=True)),
            ],
        ),
    ]
