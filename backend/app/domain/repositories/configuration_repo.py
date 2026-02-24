"""
Configuration Repository.

Handles database operations for configuration settings.
"""

from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.models.rosetta_setting_configuration import RosettaSettingConfiguration


class ConfigurationRepository:
    """Repository for configuration settings."""
    
    def __init__(self, db: Session):
        """Initialize repository."""
        self.db = db
    
    def get_by_key(self, config_key: str) -> Optional[RosettaSettingConfiguration]:
        """
        Get configuration by key.
        
        Args:
            config_key: Configuration key
            
        Returns:
            Configuration record or None
        """
        stmt = select(RosettaSettingConfiguration).where(
            RosettaSettingConfiguration.config_key == config_key
        )
        return self.db.execute(stmt).scalars().first()
    
    def get_value(self, config_key: str, default: str = "") -> str:
        """
        Get configuration value by key.
        
        Args:
            config_key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        config = self.get_by_key(config_key)
        return config.config_value if config else default
    
    def set_value(self, config_key: str, config_value: str) -> RosettaSettingConfiguration:
        """
        Set configuration value.
        
        Args:
            config_key: Configuration key
            config_value: Configuration value
            
        Returns:
            Updated or created configuration record
        """
        config = self.get_by_key(config_key)
        
        if config:
            config.config_value = config_value
        else:
            config = RosettaSettingConfiguration(
                config_key=config_key,
                config_value=config_value
            )
            self.db.add(config)
        
        self.db.commit()
        self.db.refresh(config)
        return config
    
    def get_all(self) -> list[RosettaSettingConfiguration]:
        """
        Get all configurations.
        
        Returns:
            List of all configuration records
        """
        stmt = select(RosettaSettingConfiguration).order_by(
            RosettaSettingConfiguration.config_key
        )
        return list(self.db.execute(stmt).scalars().all())
