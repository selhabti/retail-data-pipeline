#!/bin/bash

# update-readme.sh
# Script pour mettre à jour automatiquement l'arborescence dans README.md
# Conforme aux bonnes pratiques DevOps
# Version: 1.0.0

set -euo pipefail  # Fail fast sur les erreurs

# Configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly README_FILE="${SCRIPT_DIR}/README.md"
readonly BACKUP_FILE="${README_FILE}.backup"
readonly LOG_FILE="${SCRIPT_DIR}/update-readme.log"

# Couleurs pour les logs
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Logging
log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_info() {
    log "${BLUE}[INFO]${NC} $*"
}

log_success() {
    log "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    log "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    log "${RED}[ERROR]${NC} $*"
}

# Vérifier les prérequis
check_prerequisites() {
    log_info "Vérification des prérequis..."
    
    if ! command -v tree >/dev/null 2>&1; then
        log_warning "tree n'est pas installé. Installation en cours..."
        if command -v apt >/dev/null 2>&1; then
            sudo apt update && sudo apt install -y tree
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y tree
        elif command -v brew >/dev/null 2>&1; then
            brew install tree
        else
            log_error "Impossible d'installer tree automatiquement"
            exit 1
        fi
        log_success "tree installé avec succès"
    fi
    
    if ! command -v git >/dev/null 2>&1; then
        log_error "Git n'est pas installé"
        exit 1
    fi
    
    log_success "Prérequis validés"
}

# Créer une sauvegarde
create_backup() {
    if [[ -f "$README_FILE" ]]; then
        cp "$README_FILE" "$BACKUP_FILE"
        log_info "Sauvegarde créée: $(basename "$BACKUP_FILE")"
    fi
}

# Restaurer depuis la sauvegarde
restore_backup() {
    if [[ -f "$BACKUP_FILE" ]]; then
        cp "$BACKUP_FILE" "$README_FILE"
        log_info "README restauré depuis la sauvegarde"
    fi
}

# Générer l'arborescence
generate_tree() {
    log_info "Génération de l'arborescence..."
    
    # Exclusions par défaut + exclusions spécifiques au projet
    local excludes='node_modules|.git|__pycache__|.venv|venv|env|.env'
    excludes+='|dist|build|target|out|.next|.nuxt'
    excludes+='|.DS_Store|Thumbs.db|*.log|*.tmp|.cache|.pytest_cache'
    excludes+='|.coverage|htmlcov|.nyc_output|coverage|.terraform'
    excludes+='|vendor|composer.lock|package-lock.json|yarn.lock'
    
    # Ajouter les exclusions personnalisées si elles existent
    if [[ -f .treeignore ]]; then
        while IFS= read -r line; do
            [[ -n "$line" && ! "$line" =~ ^[[:space:]]*# ]] && excludes+="|$line"
        done < .treeignore
    fi
    
    tree -I "$excludes" --dirsfirst -a --charset ascii
}

# Mettre à jour le README
update_readme() {
    log_info "Mise à jour du README..."
    
    local tree_content
    tree_content=$(generate_tree)
    
    local tree_section
    tree_section=$(cat << EOF
<!-- TREE_START -->
## 📁 Structure du projet

\`\`\`
${tree_content}
\`\`\`

*Arborescence générée automatiquement le $(date '+%Y-%m-%d à %H:%M:%S')*
<!-- TREE_END -->
EOF
)
    
    if [[ -f "$README_FILE" ]]; then
        if grep -q "<!-- TREE_START -->" "$README_FILE"; then
            # Remplacer la section existante
            local temp_file
            temp_file=$(mktemp)
            
            # Utiliser awk pour un remplacement plus robuste
            awk '
                /<!-- TREE_START -->/ { 
                    print; 
                    while ((getline line < "'"${temp_file}.tree"'") > 0) print line; 
                    while (getline && !/<!-- TREE_END -->/) continue; 
                    print "<!-- TREE_END -->"
                    next 
                }
                { print }
            ' "$README_FILE" > "$temp_file"
            
            # Écrire le contenu tree dans un fichier temporaire
            echo "$tree_section" > "${temp_file}.tree"
            
            # Remplacement direct avec sed (plus simple)
            sed -i '/<!-- TREE_START -->/,/<!-- TREE_END -->/c\
'"$(echo "$tree_section" | sed 's/$/\\/')" "$README_FILE"
            
            rm -f "$temp_file" "${temp_file}.tree"
            log_success "Section arborescence mise à jour"
        else
            # Ajouter la section à la fin
            echo "" >> "$README_FILE"
            echo "$tree_section" >> "$README_FILE"
            log_success "Section arborescence ajoutée"
        fi
    else
        # Créer un nouveau README
        create_new_readme "$tree_section"
        log_success "Nouveau README créé"
    fi
}

# Créer un nouveau README
create_new_readme() {
    local tree_section="$1"
    local project_name
    project_name=$(basename "$PWD")
    
    cat > "$README_FILE" << EOF
# ${project_name}

## 📋 Description

Description de votre projet ici.

## 🚀 Installation

\`\`\`bash
# Vos instructions d'installation
\`\`\`

## 💻 Usage

\`\`\`bash
# Vos instructions d'usage
\`\`\`

${tree_section}

## 🤝 Contribution

Les contributions sont les bienvenues ! Veuillez suivre ces étapes :

1. Forkez le projet
2. Créez une branche pour votre fonctionnalité (\`git checkout -b feature/AmazingFeature\`)
3. Committez vos changements (\`git commit -m 'Add some AmazingFeature'\`)
4. Poussez vers la branche (\`git push origin feature/AmazingFeature\`)
5. Ouvrez une Pull Request

## 📝 License

Ce projet est sous licence [MIT](LICENSE).
EOF
}

# Valider les changements
validate_changes() {
    log_info "Validation des changements..."
    
    if ! grep -q "<!-- TREE_START -->" "$README_FILE" || ! grep -q "<!-- TREE_END -->" "$README_FILE"; then
        log_error "Marqueurs de section manquants"
        return 1
    fi
    
    if [[ ! -s "$README_FILE" ]]; then
        log_error "README vide après modification"
        return 1
    fi
    
    log_success "Changements validés"
}

# Nettoyer les fichiers temporaires
cleanup() {
    [[ -f "$BACKUP_FILE" ]] && rm -f "$BACKUP_FILE"
    log_info "Nettoyage effectué"
}

# Gestion des erreurs
error_handler() {
    local exit_code=$?
    log_error "Erreur détectée (code: $exit_code)"
    
    if [[ -f "$BACKUP_FILE" ]]; then
        log_warning "Restauration depuis la sauvegarde..."
        restore_backup
    fi
    
    cleanup
    exit $exit_code
}

# Usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help      Afficher cette aide
    -v, --verbose   Mode verbose
    -n, --dry-run   Simulation sans modification
    --no-backup     Ne pas créer de sauvegarde

Exemples:
    $0                  # Mise à jour normale
    $0 --dry-run        # Voir les changements sans les appliquer
    $0 --verbose        # Mode verbose

EOF
}

# Fonction principale
main() {
    local dry_run=false
    local verbose=false
    local no_backup=false
    
    # Parser les arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            -n|--dry-run)
                dry_run=true
                shift
                ;;
            --no-backup)
                no_backup=true
                shift
                ;;
            *)
                log_error "Option inconnue: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Configuration du trap pour la gestion d'erreurs
    trap error_handler ERR EXIT
    
    log_info "Démarrage de la mise à jour du README"
    log_info "Répertoire de travail: $PWD"
    
    check_prerequisites
    
    if [[ "$no_backup" == false ]]; then
        create_backup
    fi
    
    if [[ "$dry_run" == true ]]; then
        log_info "=== MODE DRY-RUN ==="
        log_info "Arborescence qui serait ajoutée:"
        echo "---"
        generate_tree
        echo "---"
        log_info "Aucune modification effectuée (dry-run)"
    else
        update_readme
        validate_changes
        log_success "README mis à jour avec succès!"
        
        if [[ "$verbose" == true ]]; then
            log_info "Aperçu de l'arborescence ajoutée:"
            echo "---"
            generate_tree
            echo "---"
        fi
    fi
    
    # Désactiver le trap d'erreur pour un nettoyage normal
    trap - ERR EXIT
    cleanup
    
    log_success "Opération terminée avec succès"
}

# Point d'entrée
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
