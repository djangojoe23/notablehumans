# Generated by Django 5.0.11 on 2025-02-08 10:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_collection', '0013_remove_notablehuman_last_updated_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='notablehuman',
            name='article_creation_date',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='notablehuman',
            name='article_length',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='notablehuman',
            name='article_quality',
            field=models.CharField(blank=True, max_length=50, null=True),
        ),
        migrations.AddField(
            model_name='notablehuman',
            name='article_recent_edits',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='notablehuman',
            name='article_recent_views',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='notablehuman',
            name='article_total_edits',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
