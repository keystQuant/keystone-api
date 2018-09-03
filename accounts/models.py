from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver


class Profile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL,
                                on_delete=models.CASCADE,
                                to_field='username',
                                related_name='profile',
                                primary_key=True)
    name = models.CharField(max_length=50, blank=True)
    phone = models.CharField(max_length=11, blank=True)
    address = models.CharField(max_length=100, blank=True)
    updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "{}".format(self.user.username)

@receiver(post_save, sender=settings.AUTH_USER_MODEL)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        Profile.objects.create(user=instance)

@receiver(post_save, sender=settings.AUTH_USER_MODEL)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()
