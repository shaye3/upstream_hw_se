#!/usr/bin/env python3
import logging
import argparse
import json
import sys
from pathlib import Path

# Add src directory to Python path
sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline_orchestrator import PipelineOrchestrator, PipelineConfig

def setup_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )


def main():
    parser = argparse.ArgumentParser(
        description="Upstream Vehicle Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--batch-size", 
        type=int,
        default=10000
    )
    
    parser.add_argument(
        "--api-url",
        default="http://localhost:9900"
    )
    
    parser.add_argument(
        "--batch-id"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO"
    )
    
    parser.add_argument(
        "--config"
    )
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    logger = logging.getLogger("main")
    
    try:
        config = PipelineConfig(
            api_base_url=args.api_url,
            batch_size=args.batch_size
        )
        
        # Override with config file if provided
        if args.config:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
                for key, value in config_data.items():
                    if hasattr(config, key):
                        setattr(config, key, value)
        
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(config)
        
        # Run pipeline stage
        logger.info(f"Starting pipeline execution")
        
        results = orchestrator.run_full_pipeline(batch_id=args.batch_id)
            
        # Print results
        print("\n" + "="*50)
        print("PIPELINE EXECUTION RESULTS")
        print("="*50)
        print(json.dumps(results, indent=2, default=str))
        
        if results["status"] == "success":
            logger.info("Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
