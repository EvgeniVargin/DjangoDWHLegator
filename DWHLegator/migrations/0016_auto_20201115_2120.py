# Generated by Django 3.1.2 on 2020-11-15 16:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DWHLegator', '0015_auto_20201110_0927'),
    ]

    operations = [
        migrations.AddField(
            model_name='entity',
            name='checkout_user',
            field=models.CharField(blank=True, max_length=256, null=True),
        ),
        migrations.AddField(
            model_name='entity',
            name='deleted',
            field=models.IntegerField(default=0),
        ),
    ]
