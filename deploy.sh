


#!/bin/bash


set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════╗"
    echo "║   Airflow 3.x Incremental ETL         ║"
    echo "║   Deployment Manager                   ║"
    echo "╚════════════════════════════════════════╝"
    echo -e "${NC}"
}

usage() {
    echo "Usage: $0 [ENVIRONMENT] [ACTION]"
    echo ""
    echo "Environments: dev, test, prod"
    echo "Actions: start, stop, restart, logs, status"
    echo ""
    echo "Examples:"
    echo "  $0 dev              # Start dev"
    echo "  $0 test logs         # View test logs"
    echo "  $0 prod restart     # Restart prod"
    exit 1
}

ENVIRONMENT=${1:-dev}
ACTION=${2:-start}

if [[ ! "$ENVIRONMENT" =~ ^(dev|test|prod)$ ]]; then
    echo -e "${RED}Error: Invalid environment '$ENVIRONMENT'${NC}"
    usage
fi

ENV_FILE="config/env_config/.env.${ENVIRONMENT}"
if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}Error: Environment file not found: $ENV_FILE${NC}"
    exit 1
fi

show_banner

set -a
source "$ENV_FILE"
set +a

export ENV_FILE_PATH="${ENV_FILE}"
export AIRFLOW_ENV="${ENVIRONMENT}"
export COMPOSE_PROJECT_NAME="airflow_${ENVIRONMENT}"

echo "%%%%%%%%%%%%%%%%%%%%%"
echo $ENV_FILE_PATH
echo $AIRFLOW_ENV

echo -e "${BLUE}Environment:${NC} ${YELLOW}${ENVIRONMENT}${NC}"
echo -e "${BLUE}Action:${NC} ${YELLOW}${ACTION}${NC}"
echo ""

case $ACTION in
    start)
        echo -e "${GREEN}Starting Airflow...${NC}"
        docker compose up -d
        echo ""
        echo -e "${GREEN}✓ Services started!${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}Airflow UI:${NC} http://localhost:8080"
        echo -e "${GREEN}Username:${NC} admin"
        echo -e "${GREEN}Password:${NC} admin"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        ;;
    stop)
        echo -e "${YELLOW}Stopping Airflow...${NC}"
        docker compose down
        echo -e "${GREEN}✓ Stopped${NC}"
        ;;
    restart)
        echo -e "${YELLOW}Restarting Airflow...${NC}"
        docker compose restart
        echo -e "${GREEN}✓ Restarted${NC}"
        ;;
    reload)
        echo -e "${YELLOW}Reload and Recreate Airflow...${NC}"
        docker compose up -d --force-recreate
        echo -e "${GREEN}✓ Restarted with updated environment${NC}"
        ;;
    logs)
        docker compose logs -f --tail=100
        ;;
    status)
        echo -e "${BLUE}Status:${NC}"
        docker compose ps
        ;;
    start)
        echo -e "${YELLOW}Building image (if needed)...${NC}"
        docker compose build
        
        echo -e "${GREEN}Starting Airflow...${NC}"
        docker compose up -d
        
        echo -e "${GREEN}✓ Services started!${NC}"
        ;;
    
    rebuild)
        echo -e "${YELLOW}Rebuilding image from scratch...${NC}"
        docker compose build --no-cache
        
        echo -e "${YELLOW}Restarting services...${NC}"
        docker compose up -d --force-recreate
        
        echo -e "${GREEN}✓ Rebuild complete!${NC}"
        ;;
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        usage
        ;;
    
    # ... (rest of cases)
esac
