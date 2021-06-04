# Generated by Django 3.1.2 on 2020-11-06 12:45

from django.db import migrations, models
import django.db.models.deletion
import mptt.fields


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('DWHLegator', '0002_delete_group'),
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
        migrations.CreateModel(
            name='Label',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('caption', models.CharField(max_length=256, unique=True)),
                ('ord', models.IntegerField()),
                ('soft_type', models.CharField(max_length=256)),
                ('lft', models.PositiveIntegerField(editable=False)),
                ('rght', models.PositiveIntegerField(editable=False)),
                ('tree_id', models.PositiveIntegerField(db_index=True, editable=False)),
                ('level', models.PositiveIntegerField(editable=False)),
                ('parent', mptt.fields.TreeForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='children', to='DWHLegator.label')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]